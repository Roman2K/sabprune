require_relative 'imports'
require_relative 'dl_dir'

module Incomplete

class Pruner
  def initialize(ng, mnt, sab:, log:)
    @log = log

    @dls = find_downloads Pathname(ng), Pathname(mnt)

    sab.queue.each do |ev|
      add_sab_queue_ev(ev)
    end
    sab.history.each do |ev|
      add_sab_hist_ev(ev)
      break if @dls.values.all? &:status
    end
  end

  private def add_sab_queue_ev(ev)
    dl = @dls[ev.fetch "filename"] or return
    dl.status = :queued
    dl.log.debug "found in SABnzbd queue"
  end

  private def add_sab_hist_ev(ev)
    dl = @dls[ev.fetch "name"] or return
    dl.status = ev.fetch("status").yield_self do |st|
      case st
      when "Completed", "Failed" then :ended
      else :other
      end
    end
    dl.log[status: dl.status].info "found in SABnzbd history"
  end

  def add_ev(pvr, ev); end
  def need_evs?; false end

  def prune
    freed = count = 0
    @dls.each do |_, dl|
      unless [nil, :ended].include? dl.status 
        dl.log[status: dl.status].debug "not deleting dirs"
        next
      end
      dl.dirs.each do |dir|
        size = dir.size
        dl.log[dir: dir.mnt.basename].
          info "deleting #{(dir.status || "unknown").upcase} dir" do
            Imports::Pruner.fu :rm_r, dir.local
          end
        count += 1
        freed += size if size
      end
    end
    @log.info "freed %s by deleting %d dirs" % [
      Utils::Fmt.size(freed),
      count,
    ]
  end

  private def find_downloads(root, mnt)
    found = {}
    root.join(Imports::Pruner::INCOMPLETE_DIR).glob("*") do |f|
      next unless f.directory?
      name = f.basename.to_s.sub /\.\d+$/, ""  # handle xxx.1 dirs
      dir = DLDir.from_local f, root: root, mnt: mnt
      dl = found[name] ||= Download.new.tap { |d|
        d.dirs = []
        d.log = @log[name: dir.mnt.dirname.join("#{name}*")]
      }
      dl.dirs << DLDir.from_local(f, root: root, mnt: mnt)
    end
    found
  end
end

Download = Struct.new :dirs, :log, :status

class SABnzbd
  def initialize(uri, log:)
    uri = Utils.merge_uri uri, output: "json"
    @http = Utils::SimpleHTTP.new uri, log: log
    @http.type_config.update json_in: false, json_out: true
  end

  def history &block; get_slots({mode: "history"}, "history", &block) end
  def queue &block; get_slots({mode: "queue"}, "queue", &block) end

  LIMIT = 500

  private def get_slots(params, key, &block)
    return enum_for __method__, params, key unless block
    start = 0
    last_nzoid = nil
    loop do
      items = @http.get([params.merge(start: start, limit: LIMIT)]).
        fetch(key).
        fetch("slots")
      if last_nzoid
        idx = items.find_index { |i| i.fetch("nzo_id") == last_nzoid } \
          or raise "slots moved too much since last iteration"
        start += idx
        items.slice! 0..idx
      end
      break if items.empty?
      items.each &block
      start += items.size - 1
      last_nzoid = items.fetch(-1).fetch "nzo_id"
    end
  end
end

end # Incomplete
