require 'fiber'
require_relative 'imports'
require_relative 'incomplete'

class App
  def initialize(ng, mnt, sab:, pvrs: [], log:)
    @sab = sab
    @pvrs = pvrs
    @pruners = [
      # -> { Incomplete::Pruner.new(ng) },
      -> { Imports::Pruner.new(ng, mnt, log: log) },
    ]
  end

  def cmd_prune
    pruners = @pruners.map &:call
    each_ev do |pvr, ev|
      cont = true
      pruners.each do |pr|
        pr.add_ev pvr, ev
        cont &&= pr.need_evs?
      end
      cont or break
    end
    pruners.each &:prune
  end

  private def each_ev
    hists = @pvrs.map do |pvr|
      evs = pvr.history_events
      Fiber.new { evs.each { |ev| Fiber.yield pvr, ev } }
    end
    loop do
      hists.delete_if do |h|
        item = h.resume
        h.alive? or next true
        yield item
        false
      end
      break if hists.empty?
    end
  end
end

if $0 == __FILE__
  require 'metacli'
  config = Utils::Conf.new "config.yml"
  log = Utils::Log.new $stderr, level: :info
  log.level = :debug if ENV["DEBUG"] == "1"
  sab = Incomplete::SABnzbd.new URI(config[:sab]), log: log["sab"]
  pvrs = config[:pvrs].to_hash.map do |name, url|
    Utils::PVR.const_get(name).new(URI(url), log: log[name])
  end
  app = App.new config[:ng], config[:mnt],
    pvrs: pvrs,
    sab: sab,
    log: log
  MetaCLI.new(ARGV).run app
end
