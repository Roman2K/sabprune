require_relative 'imports'
require_relative 'dl_dir'

module Incomplete

class Pruner
  def initialize(ng, mnt, sab:, log:)
    @log = log

    @dls = find_downloads Pathname(ng), Pathname(mnt)

    sab.queue.fetch("queue").fetch("slots").each do |ev|
      add_sab_queue_ev(ev)
    end
    sab.history.fetch("history").fetch("slots").each do |ev|
      add_sab_hist_ev(ev)
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

  def history; @http.get([mode: "history"]) end
  def queue; @http.get([mode: "queue"]) end
end

end # Incomplete
