require 'utils'
require 'fileutils'

class App
  FU = FileUtils
  FU_OPTS = {
    noop: false,
    verbose: false,
  }

  def initialize(ng, mnt, pvrs: {}, log:)
    @ng = Pathname ng
    @mnt = Pathname mnt
    @pvrs = pvrs
    @log = log
  end

  def cmd_prune
    imports = find_imports.group_by &:status
    @log.info "stats: %p" % stats(imports)

    ##
    # Cleanup
    #
    imports.delete(nil).each do |imp|
      imp.log.warn "found ORPHAN dir"
    end
    %i[empty junk imported].each do |st|
      imports.delete(st) { [] }.each do |imp|
        imp.log.info "deleting #{st.upcase} dir" do
          fu :rm_r, imp.dir.local
        end
      end
    end

    ##
    # Queue imports
    #
    commands = Commands.new
    imports.delete(:grabbed) { [] }.group_by(&:pvr).each do |pvr, imps|
      existing = pvr.commands.
        select { |c| c.fetch("name") == pvr.class::CMD_DOWNLOADED_SCAN }.
        sort_by { |c| c.fetch("started") }
      imps.each do |imp|
        imp_log = imp.log["import"]
        cmd = existing.
          find { |c| c.fetch("body").fetch("path") == imp.dir.mnt.to_s }
        if cmd
          imp_log[id: cmd.fetch("id")].info "found running import command"
        else
          imp_log.warn "running command"
          cmd = pvr.downloaded_scan imp.dir.mnt,
            download_client_id: imp.nzoid, import_mode: 'Move'
        end
        commands << Commands::Cmd[pvr, cmd.fetch("id"), imp]
      end
    end

    ##
    # Refresh statuses
    #
    if (io = $stdout).tty?
      commands.live_statuses(io) { |imp| imp.dir.mnt }
    end

    ##
    # Wait for imports
    #
    commands.wait
    commands.last_statuses.each do |cmd, st|
      imp = cmd.obj
      if !imp.dir.local.directory?
        imp.log[status: st].info "import command completed"
        next
      end
      imp.log.warn "directory still present, checking PVR"
      if !imp.entity.fetch("hasFile")
        imp.log.error "PVR doesn't have files: import command failed"
        next
      end
      imp.log.info "PVR has files: deleting" do
        fu :rm_r, imp.dir.local
      end
    end

    ##
    # Sanity check
    #
    unless imports.empty?
      @log.error "unknown imports left: %s" % [PP.pp(imports, "").chomp]
    end
  end

  private def fu(*args, **opts, &block)
    FU.public_send *args, **FU_OPTS, **opts, &block
  end

  private def stats(imports)
    imports.each_with_object({}) do |(st, imps), h|
      h[st] =
        case st
        when :grabbed, :imported
          imps.
            group_by { |imp| imp.pvr.class.name.split("::").last }.
            transform_values &:size
        else
          imps.size
        end
    end
  end

  private def find_imports
    entries = {}.tap do |h|
      @ng.glob("*").each do |f|
        basename = f.basename.to_s
        next if f.directory? && basename == "incomplete"
        imp = Import.new
        imp.dir = Import::Dir[f, @mnt + f.relative_path_from(@ng)]
        imp.log = @log[mnt: imp.dir.mnt.to_s]
        imp.status = imp.dir.status
        h[basename] = imp
      end
    end
    @pvrs.each do |pvr_name, pvr|
      @log[pvr: pvr_name].info("fetching history") {
        pvr.history
      }.each { |ev|
        ev.fetch("data")&.[]("downloadClient")&.downcase == "sabnzbd" or next
        imp = entries[ev.fetch "sourceTitle"] or next
        raise "PVR mismatch" unless [nil, pvr].include? imp.pvr
        imp.log = imp.log[pvr: pvr_name]
        next if %i[empty junk].include? imp.status
        imp.pvr = pvr
        imp.entity_id = pvr.history_entity_id(ev)
        cur_date = ev.fetch "date"
        imp.date.nil? || imp.date < cur_date or next
        imp.date = cur_date
        imp.nzoid = ev.fetch("downloadId")
        imp.status = ev.fetch("eventType").yield_self do |st|
          case st
          when 'grabbed' then :grabbed
          when 'downloadFolderImported' then :imported
          else raise "unknown status: %p" % [st]
          end
        end
      }
    end
    entries.values
  end
end

class Commands < Array
  Cmd = Struct.new :pvr, :id, :obj

  DEFAULT_REFRESH_PERIOD = 1

  def live_statuses(io, refresh: DEFAULT_REFRESH_PERIOD)
    print = Fiber.new { print_live_statuses(io) }
    print.resume
    loop do
      done = true
      print.resume statuses.map { |cmd, st|
        done &&= st.final?
        s = cmd.obj
        s = yield s if block_given?
        [s, st]
      }
      break if done
      sleep refresh
    end
    print.resume
  end
    
  def print_live_statuses(io)
    io = Utils::IOUtils::Refresh.new io
    last = Time.now

    while sts = Fiber.yield
      now = Time.now
      elapsed = now - last
      last = now

      tbl = Utils::IOUtils::Table.new
      tbl.col(0).align = :left
      sts.
        sort_by { |s, st| [s.to_s.downcase, st] }.
        each { |s, st| tbl << [s, st] }

      io.puts
      io.puts "Last refresh: %s" % [Utils::Fmt.duration(elapsed)]
      io.puts "Running commands:"
      tbl.write_to io
      io.flush
    end

    io.flush
  end

  def statuses
    @last_statuses = get_statuses.to_a
  end

  private def get_statuses
    return enum_for :get_statuses unless block_given?
    group_by(&:pvr).each do |pvr, cmds|
      cmds = cmds.each_with_object({}) { |cmd,h|
        h[cmd.id] and raise "duplicate cmd"
        h[cmd.id] = cmd
      }
      pvr.commands.each do |raw|
        cmd = cmds.delete(raw.fetch "id") or next
        yield cmd, Status.new(raw.fetch "state")
      end
      cmds.inject(Queue.new) { |q, kv| q << kv }.close.tap do |q| 
        Array.new(8) {
          Thread.new do
            Thread.current.abort_on_exception = true
            while (id, cmd = q.shift)
              raw = pvr.command id
              yield cmd, Status.new(raw.fetch "state")
            end
          end
        }.each &:join
      end
    end
  end

  def last_statuses
    @last_statuses || statuses
  end

  def wait(refresh: DEFAULT_REFRESH_PERIOD)
    sts = last_statuses
    until sts.all? { |cmd, st| st.final? }
      sleep refresh
      sts = statuses 
    end
  end
end

Status = Struct.new(:name) do
  def final?; name == "completed" end
  def to_s; name.to_s end
end

class Import < Struct.new(:pvr, :entity_id, :dir, :log, :status, :date, :nzoid,
  keyword_init: true,
)
  def entity
    pvr.entity entity_id
  end

  Dir = Struct.new :local, :mnt do
    def status
      case
      when empty? then :empty
      when junk? then :junk
      end
    end

    private def empty?
      local.glob("**/*") { return false }
      true
    end

    private def junk?
      local.enum_for(:glob, "*").all? do |f|
        f.file? && f.basename.to_s =~ /^\d+-\d+(\.\d+)+$/
      end
    end
  end
end

if $0 == __FILE__
  require 'metacli'
  config = Utils::Conf.new "config.yml"
  log = Utils::Log.new $stderr, level: :info
  log.level = :debug if ENV["DEBUG"] == "1"
  pvrs = config[:pvrs].to_hash.each_with_object({}) do |(name, url), h|
    h[name] = Utils::PVR.const_get(name.capitalize).new(URI(url), log: log)
  end
  app = App.new config["ng"], config["mnt"], pvrs: pvrs, log: log
  MetaCLI.new(ARGV).run app
end
