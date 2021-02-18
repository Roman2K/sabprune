require 'utils'
require 'fileutils'
require 'pp'
require 'time'
require_relative 'dl_dir'

module Imports

class Pruner
  IMPORT_GRACE = 12 * 3600
  UNPACK_GRACE = 4 * 3600

  FU = FileUtils
  FU_OPTS = {
    noop: false,
    verbose: false,
  }

  def self.fu(*args, **opts, &block)
    FU.public_send *args, **FU_OPTS, **opts, &block
  end

  def initialize(ng, mnt, dest_dir, log:)
    @import_grace = IMPORT_GRACE
    @unpack_grace = UNPACK_GRACE
    @log = log

    @dest_dir = Pathname(dest_dir)
    @imports = find_imports Pathname(ng), Pathname(mnt)
  end

  attr_accessor \
    :import_grace,
    :unpack_grace

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
          info "not superseding similarly-named dir, would delete on next run"
      else
        entries[basename] = imp
      end
    end
    entries
  end

  private def find_ev_import(ev)
    title = ev.fetch "sourceTitle"
    alts = [title]
    # Handle releases named with a trailing dot in the PVR
    # (`Home.on.the.Range.2004.1080p.BluRay.DTS.x264-CyTSuNee.`) but get saved
    # in a directory without the dot by SABnzbd.
    if title =~ /\.+$/
      alts << $`
    end
    alts.each do |t|
      imp = @imports[t] and return imp
    end
    nil
  end

  def add_ev(pvr, ev)
    ev.fetch("data")&.[]("downloadClient")&.downcase == "sabnzbd" or return

    imp = find_ev_import(ev) or return
    raise "PVR mismatch" unless [nil, pvr].include? imp.pvr

    cur_date = Time.parse ev.fetch "date"
    return if imp.date && imp.date > cur_date

    imp.log = imp.log[pvr: pvr.name]
    return if %i[empty junk].include? imp.status

    imp.pvr = pvr
    imp.entity_id = pvr.history_entity_id(ev)
    imp.scannable_id = pvr.history_scannable_id(ev)
    imp.dest_dir = begin
      @dest_dir.join(pvr.history_dest_path(ev).sub(%r[^/+], ""))
    rescue NotImplementedError
    end

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
      @counts = {imports: 0, deletions: 0, invalid_dl: 0}
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
    imports[:orphan] = imports.delete(nil) { [] }
    %i[empty junk imported dl_failed orphan].each do |st|
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
          size: Fmt.size_or_nil(sizes_before[imp] = imp.dir.size),
        ].debug "calculated size before import"
        cmd = existing.
          find { |c| c.fetch("body").fetch("path") == imp.dir.mnt.to_s }
        if cmd
          imp_log[id: cmd.fetch("id")].info "found running import command"
        else
          imp_log.info "running command"
          cmd = pvr.downloaded_scan imp.dir.mnt, download_client_id: imp.nzoid,
            import_mode: :move
        end
        commands << Commands::Cmd.new(pvr, imp).tap { _1.id = cmd.fetch "id" }
      end
    end

    ##
    # Sanity check
    #
    unless imports.empty?
      @log.error "unknown imports left: %s" % [PP.pp(imports, "").chomp]
    end

    ##
    # Check results
    #
    mark_failed = []
    until commands.empty?
      statuses = commands.wait
      commands.clear
      statuses.each do |cmd, st|
        imp = cmd.obj
        st = imp.check_result(st, grace: @import_grace)
        log = imp.log[import_status: st]
        freed = sizes_before[imp]
        case st
        when /^ok/
          log.info "imported %s" % Fmt.size_or_nil(freed)
          stats.add :imports, freed
        when :err, :grace
          if cmd.exec_count > 1
            log.error "forced import failed"
          else
            log.info "normal import failed, forcing import"
            begin
              imp.copy_to_dest
            rescue Import::CopyError
              log[err: $!].error "couldn't copy files"
            else
              log.info "requesting PVR #{cmd.pvr} to scan files"
              cmd.id = cmd.pvr.rescan(imp.scannable_id).fetch("id")
              commands << cmd
            end
          end
        when :invalid_dl
          log.info "deleted %s of invalid download" % Fmt.size_or_nil(freed)
          mark_failed << imp
          stats.add :invalid_dl, freed
        else
          raise "unhandled status: %p" % [st]
        end
      end
    end
    @log.info "freed %s after %s" % [
      Utils::Fmt.size(stats.total_freed),
      stats.counts.map { |s,n| "%d %s" % [n,s] }.join(", "),
    ]

    ##
    # Mark failed
    #
    queues = Hash.new { |cache, pvr| cache[pvr] = pvr.queue }
    failed_count = 0
    mark_failed.each do |imp|
      qid = queues[imp.pvr].
        find { id = _1["downloadId"] and id.downcase == imp.nzoid.downcase } \
        &.fetch "id"
      unless qid
        imp.log.warn "item not found in PVR queue, couldn't mark failed"
        next
      end
      imp.log[queue_item: qid].info "deleting from queue and blacklisting" do
        imp.pvr.queue_del qid, blacklist: true
        failed_count += 1
      end
    end
    @log.info "marked %d downloads as failed" % [failed_count]
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

  module Fmt
    def self.size_or_nil(size)
      size ? Utils::Fmt.size(size) : "???"
    end
  end
end

class Import < Struct.new(
  :pvr, :nzoid, :entity_id, :scannable_id, :status, :dir, :dest_dir, :date,
  :log,
  keyword_init: true,
)
  def entity
    pvr.entity entity_id
  end

  def copy_to_dest
    dest_dir \
      or raise CopyError, "dest_dir not set, copy not supported for #{pvr.name}"
    dest_dir.dirname.directory? \
      or raise CopyError, "parent directory missing for #{dest_dir}"

    src_files = dir.local.children.select &:file?
    !src_files.empty? or raise CopyError, "source dir is empty"

    Pruner.fu :mkdir_p, dest_dir
    if (dst_sz = dest_dir.children.select(&:file?).sum(&:size)) \
      >= (src_sz = src_files.sum(&:size)) \
    then
      log[{src: src_sz, dest: dst_sz}.transform_values { Utils::Fmt.size _1 }].
        warn "destination >= source, not copying"
      return
    end

    log[dest: dest_dir].info "copying #{src_files.size} files" do
      system "rsync", "-a", *src_files.map(&:to_s), "#{dest_dir}/" \
        or raise "rsync failed"
    end
  end

  class CopyError < StandardError; end

  def check_result(st, grace:)
    log[status: st].info "import command finished"

    if st.error?
      log.error "import command failed, not checking directory"
      return :err
    end

    if !dir.local.directory?
      log.info "import succeeded"
      return :ok
    end
    status = :ok_leftovers

    if !entity.fetch("hasFile")
      if (age = Time.now - [date, dir.contents_mtime].max) <= grace
        log.info "directory still present, allowing %s" \
          % [Utils::Fmt.duration(grace - age)]
        return :grace
      end
      log.warn "PVR doesn't have files after %s: invalid download" \
        % [Utils::Fmt.duration(age)]
      status = :invalid_dl
    end

    log.info "deleting leftover files after import" do
      # Ignore errors due to late deletion by the PVR
      Pruner.fu :rm_rf, dir.local
    end
    status
  end
end

class Commands < Array
  Cmd = Struct.new :pvr, :obj do
    def initialize *; super; @exec_count = 0 end
    def id=(id); old, @id = @id, id; @exec_count += 1 if id && id != old end
    attr_reader :id, :exec_count
  end

  def statuses; @last_statuses = get_statuses.to_a end
  def clear; @last_statuses = nil; super end

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

  DEFAULT_REFRESH_PERIOD = 1

  def wait(refresh: DEFAULT_REFRESH_PERIOD)
    sts = last_statuses
    until sts.all? { |cmd, st| st.final? }
      sleep refresh
      sts = statuses 
    end
    sts
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
