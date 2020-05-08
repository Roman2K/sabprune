require 'minitest/autorun'
require_relative 'imports'

module Imports

class PrunerTest < Minitest::Test
  MNT = "/ng"

  def setup
    @pvr = TestPVR.new
    @ng = Pathname(Dir.mktmpdir)
  end

  private def prune
    log = Utils::Log.new \
      @log_io = StringIO.new,
      level: :debug
    pruner = Pruner.new @ng, MNT, log: log do |p|
      p.status_io = File.open "/dev/null", 'w'
      p.import_grace = IMPORT_GRACE
      p.unpack_grace = UNPACK_GRACE
    end

    @pvr.history.each { |ev| pruner.add_ev @pvr, ev }
    pruner.prune
  end

  def teardown
    FileUtils.rm_r @ng
  end

  def test_prune_empty_dir
    @ng.join("_some empty dir").mkdir

    prune

    assert_log :info, "deleting empty.*some empt"
  end

  def test_import_success_leftovers
    dir = "ep02"
    local_dir = @ng.join(dir).tap do |d|
      d.mkdir
      d.join("some file").write "test"
    end
    @pvr.add_ev Pruner::ST_GRABBED, {
      'date' => Time.now.to_s,
      'data' => {'downloadClient' => "sabnzbd"},
      'downloadId' => 'nzoid02',
      'sourceTitle' => dir,
    }
    @pvr.add_import "#{MNT}/#{dir}", Status::COMPLETED
    @pvr.set_has_file dir, true

    prune

    assert_equal %W[ #{MNT}/#{dir} ], @pvr.imported
    refute local_dir.exist?
    assert_log :info, "deleting leftover files.+mnt=#{MNT}/#{dir}"
  end

  IMPORT_GRACE = 4

  def test_import_success_no_hasFile_grace
    dir = "ep03"
    local_dir = @ng.join(dir).tap do |d|
      d.mkdir
      d.join("some file").write "test"
    end
    @pvr.add_ev Pruner::ST_GRABBED, {
      'date' => (Time.now - 999).to_s,
      'data' => {'downloadClient' => "sabnzbd"},
      'downloadId' => 'nZoiD03',
      'sourceTitle' => dir,
    }
    @pvr.add_import "#{MNT}/#{dir}", Status::COMPLETED
    @pvr.set_has_file dir, false

    ##
    # Grace NOT expired
    #
    prune

    assert_equal %W[ #{MNT}/#{dir} ], @pvr.imported
    assert local_dir.exist?
    assert_log :info, ".*\\bstill present, allowing.+mnt=#{MNT}/#{dir}"

    ##
    # Grace expired, mark failed IN QUEUE
    #
    touch_dir_and_contents local_dir, mtime: Time.now - 5
    @pvr.add_to_queue "123", "NzOId03"

    prune

    assert_equal %W[ #{MNT}/#{dir} ] * 2, @pvr.imported
    refute local_dir.exist?
    assert_equal ["123"], @pvr.queue_dels
    assert_log :warn, ".*\\bdoesn't have files after.+mnt=#{MNT}/#{dir}"
    assert_log :info, ".*\\bdeleting from queue.+queue_item=123"
  end

  def touch_dir_and_contents(dir, **opts)
    FileUtils.touch [dir, *dir.glob("*")], **opts
  end

  UNPACK_GRACE = 2

  def test_unpack
    title = "ep01"
    dir = "_UNPACK_#{title}"
    local_dir = @ng.join(dir).tap do |d|
      d.mkdir
      d.join("some file").write "test"
    end
    @pvr.add_ev Pruner::ST_GRABBED, {
      'date' => (Time.now - 999).to_s,
      'data' => {'downloadClient' => "sabnzbd"},
      'downloadId' => 'nzoid01',
      'sourceTitle' => title,
    }

    ##
    # Grace NOT expired
    #
    prune

    assert_equal %W[ ], @pvr.imported
    assert_log :debug, ".*\\bunpacking.+mnt=#{MNT}/#{dir}"

    ##
    # Grace expired
    #
    touch_dir_and_contents local_dir, mtime: Time.now - 3
    @pvr.add_import "#{MNT}/#{dir}", Status::COMPLETED
    @pvr.set_has_file title, true

    prune

    assert_equal %W[ #{MNT}/#{dir} ], @pvr.imported
  end

  private def assert_log(*args, **opts)
    assert_match Utils::Log.matcher(*args, **opts), @log_io.string
  end

  class TestPVR
    def initialize
      @events = []
      @imports = {}
      @queue = []
      @has_files = {}

      @imported = []
      @queue_dels = []
    end

    attr_reader \
      :imported,
      :queue_dels

    def name; "test_pvr" end
    def history; @events end
    def queue; @queue end
    def add_ev(type, ev); @events << ev.merge("eventType" => type) end
    def add_import(mnt_dir, st); @imports[mnt_dir.to_s] = st end
    def add_to_queue(id, dl_id); @queue << {"id"=>id, "downloadId"=>dl_id} end
    def queue_del(id, blacklist:); @queue_dels << id end
    def command(id); {"state" => @imports.fetch(id)} end
    def set_has_file(entity_id, ok); @has_files[entity_id] = ok end
    def entity(id); {"hasFile" => @has_files.fetch(id)} end
    def history_entity_id(ev); ev.fetch "sourceTitle" end
    def commands; [] end  # all finished

    def downloaded_scan(mnt_dir, **opts)
      mnt_dir = mnt_dir.to_s
      @imports.key? mnt_dir \
        or raise "unexpected downloaded_scan: %p" % [mnt_dir]
      @imported << mnt_dir
      {"id" => mnt_dir}
    end
  end
end # PrunerTest

class CommandsTest < Minitest::Test
  def test_live_statuses
    io = StringIO.new
    pvr = TestPVR.new commands_responses: [
      [ {"id" => 99, "state" => "pending"},
        {"id" => 1, "state" => "queued"} ],
      [ {"id" => 99, "state" => "pending"},
        {"id" => 1, "state" => "completed"} ],
    ]
    cmds = Commands[
      Commands::Cmd[pvr, 1, "myobj"]
    ]

    cmds.live_statuses io, refresh: 0.01

    assert_equal 2, io.string.scan(/myobj/).size
    assert_equal 1, io.string.scan(/queued/).size
    assert_equal 1, io.string.scan(/completed/).size

    cmds.wait refresh: 60   # doesn't block
  end

  def test_wait
    pvr = TestPVR.new commands_responses: [
      [ {"id" => 99, "state" => "pending"},
        {"id" => 1, "state" => "queued"} ],
      [ {"id" => 99, "state" => "pending"},
        {"id" => 1, "state" => "completed"} ],
    ], command_responses: {
      2 => [
        {"id" => 2, "state" => "pending"},
        {"id" => 2, "state" => "completed"},
        {"id" => 2, "state" => "xxx"},
      ],
    }
    cmds = Commands[
      Commands::Cmd[pvr, 1, "foo"],
      Commands::Cmd[pvr, 2, "bar"],
    ]

    cmds.wait refresh: 0.01

    assert_equal [], pvr.commands_responses
    assert_equal ["xxx"],
      pvr.command_responses.fetch(2).map { |h| h.fetch("state") }
  end

  TestPVR = Struct.new(
    :commands_responses, :command_responses, keyword_init: true,
  ) do
    def commands
      commands_responses.shift || []
    end

    def command(id)
      command_responses.fetch(id).shift or raise "no command response left"
    end
  end
end # CommandsTest

end # Imports
