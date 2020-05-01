require 'utils'
require 'fileutils'
require 'pp'
require_relative 'dl_dir'

module Imports

class Pruner
  IMPORT_GRACE = 4 * 3600
  UNPACK_GRACE = 2 * 3600

  FU = FileUtils
  FU_OPTS = {
    noop: false,
    verbose: false,
  }

  def self.fu(*args, **opts, &block)
    FU.public_send *args, **FU_OPTS, **opts, &block
  end

  def initialize(ng, mnt, log:)
    @import_grace = IMPORT_GRACE
    @unpack_grace = UNPACK_GRACE
    @status_io = $stdout
    @log = log
    yield self if block_given?
    @imports = find_imports Pathname(ng), Pathname(mnt)
  end

  attr_accessor \
    :import_grace,
    :unpack_grace,
    :status_io

  INCOMPLETE_DIR = "incomplete"

  private def find_imports(root, mnt)
    entries = {}
    root.glob("*").sort.each do |f|
      next unless f.directory?
      basename = f.basename.to_s
      next if basename == INCOMPLETE_DIR
      imp = Import.new
      imp.dir = DLDir.from_local f, root: root, mnt: mnt
      imp.log = @log[mnt: imp.dir.mnt]
      if basename =~ /^_UNPACK_/
        basename = $'
        if (rem = @unpack_grace - (Time.now - imp.dir.contents_mtime)) > 0
          imp.log.debug "unpacking, allowing %s" % [Utils::Fmt.duration(rem)]
          next
        end
      end
      imp.status = imp.dir.status
      basename.sub! /\.\d+$/, ""  # handle xxx.1 dirs
      if found = entries[basename]
        imp.log[found: found.dir.mnt.basename].
          info "superseding similarly-named dir, would delete on next run"
      end
      entries[basename] = imp
    end
    entries
  end

  def add_ev(pvr, ev)
    ev.fetch("data")&.[]("downloadClient")&.downcase == "sabnzbd" or return
    imp = @imports[ev.fetch "sourceTitle"] or return
    raise "PVR mismatch" unless [nil, pvr].include? imp.pvr
    imp.log = imp.log[pvr: pvr.name]
    return if %i[empty junk].include? imp.status
    imp.pvr = pvr
    imp.entity_id = pvr.history_entity_id(ev)
    cur_date = ev.fetch "date"
    imp.date.nil? || imp.date < cur_date or return
    imp.date = cur_date
    imp.nzoid = ev.fetch("downloadId")
    imp.status = ev.fetch("eventType").yield_self do |st|
      case st
      when ST_GRABBED then :grabbed
      when ST_IMPORTED then :imported
      when ST_FAILED then :dl_failed
      else raise "unknown status: %p" % [st]
      end
    end
  end

  def need_evs?
    @imports.any? { |_, imp| !imp.status }
  end

  class FreedStats
    def initialize
      @counts = {imports: 0, deletions: 0}
      @total_freed = 0
    end

    attr_reader :counts, :total_freed

    def add(type, size)
      @counts[type] = @counts.fetch(type) + 1
      @total_freed += size if size
      self
    end
  end

  def prune
    imports = @imports.values.group_by &:status
    @log.info "stats: %p" % import_stats(imports)
    stats = FreedStats.new

    ##
    # Cleanup
    #
    imports.delete(nil) { [] }.each do |imp|
      imp.log.warn "found ORPHAN dir"
    end
    %i[empty junk imported dl_failed].each do |st|
      imports.delete(st) { [] }.each do |imp|
        size = imp.dir.size
        imp.log.info "deleting #{st.upcase} dir" do
          Pruner.fu :rm_r, imp.dir.local
        end
        stats.add :deletions, size
      end
    end

    ##
    # Queue imports
    #
    commands = Commands.new
    sizes_before = {}
    imports.delete(:grabbed) { [] }.group_by(&:pvr).each do |pvr, imps|
      existing = pvr.commands.
        select { |c| c.fetch("name") == pvr.class::CMD_DOWNLOADED_SCAN }.
        sort_by { |c| c.fetch("started") }
      imps.each do |imp|
        imp_log = imp.log["import"]
        imp_log[
          size: sizes_before[imp] = imp.dir.size
        ].debug "calculated size before import"
        cmd = existing.
          find { |c| c.fetch("body").fetch("path") == imp.dir.mnt.to_s }
        if cmd
          imp_log[id: cmd.fetch("id")].info "found running import command"
        else
          imp_log.info "running command"
          cmd = pvr.downloaded_scan imp.dir.mnt,
            download_client_id: imp.nzoid, import_mode: 'Move'
        end
        commands << Commands::Cmd[pvr, cmd.fetch("id"), imp]
      end
    end

    ##
    # Refresh statuses
    #
    if @status_io.tty?
      commands.live_statuses(@status_io) { |imp| imp.dir.mnt }
    end

    ##
    # Check results
    #
    commands.wait
    commands.last_statuses.each do |cmd, st|
      cmd.obj.check_result(st, grace: @import_grace) or next
      freed = sizes_before[cmd.obj]
      cmd.obj.log.info "freed %s" % [freed ? Utils::Fmt.size(freed) : "???"]
      stats.add :imports, freed
    end
    @log.info "freed %s after %s" % [
      Utils::Fmt.size(stats.total_freed),
      stats.counts.map { |s,n| "%d %s" % [n,s] }.join(", "),
    ]

    ##
    # Sanity check
    #
    unless imports.empty?
      @log.error "unknown imports left: %s" % [PP.pp(imports, "").chomp]
    end
  end

  private def import_stats(imports)
    imports.each_with_object({}) do |(st, imps), h|
      h[st] =
        case st
        when :grabbed, :imported, :dl_failed
          imps.group_by { |imp| imp.pvr.name }.transform_values &:size
        else
          imps.size
        end
    end
  end

  ST_GRABBED = 'grabbed'
  ST_IMPORTED = 'downloadFolderImported'
  ST_FAILED = 'downloadFailed'
end

class Import < Struct.new(:pvr, :entity_id, :dir, :log, :status, :date, :nzoid,
  keyword_init: true,
)
  def entity
    pvr.entity entity_id
  end

  def check_result(st, grace:)
    log[status: st].info "import command finished"

    if st.error?
      log.error "import command failed, not checking directory"
      return
    end

    if !dir.local.directory?
      log.info "import succeeded"
      return
    end

    if !entity.fetch("hasFile")
      if (age = Time.now - dir.contents_mtime) > grace
        log.error "PVR doesn't have files after %s: import failed" \
          % [Utils::Fmt.duration(age)]
      else
        log.info "directory still present, allowing %s" \
          % [Utils::Fmt.duration(grace - age)]
      end
      return
    end

    log.info "deleting leftover files after import" do
      # Ignore errors due to late deletion by the PVR
      Pruner.fu :rm_rf, dir.local
    end
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
    
  private def print_live_statuses(io)
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
      io.puts "Last refresh: %s ago" % [Utils::Fmt.duration(elapsed)]
      io.puts "Running commands:"
      tbl.write_to io
      io.flush
    end
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

class Status < Struct.new :name
  PROCESSING = %w[started queued]
  COMPLETED = "completed"
  def processing?; PROCESSING.include? name end
  def final?; !processing? end
  def error?; final? && name != COMPLETED end
  def to_s; name.to_s end
end

end # Imports
