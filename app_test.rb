$:.unshift __dir__
require 'minitest/autorun'
require 'main'
require 'fileutils'

class AppTest < Minitest::Test
  MNT = "/ng"

  def setup
    @pvr = TestPVR.new
    @ng = Pathname(Dir.mktmpdir)
    @app = App.new @ng, MNT,
      log: Utils::Log.new(@log_io = StringIO.new, level: :debug),
      pvrs: {TestPVR: @pvr}
    @app.status_io = File.open "/dev/null", 'w'
  end

  def teardown
    FileUtils.rm_r @ng
  end

  def test_prune_empty_dir
    @ng.join("_some empty dir").mkdir

    @app.cmd_prune

    assert_log :info, "deleting empty.*some empt"
  end

  def test_import_success_leftovers
    dir = "ep02"
    @ng.join(dir).tap do |d|
      d.mkdir
      d.join("some file").write "test"
    end
    @pvr.add_ev App::ST_GRABBED, {
      'date' => '2020',
      'data' => {'downloadClient' => "sabnzbd"},
      'downloadId' => 'nzoid02',
      'sourceTitle' => dir,
    }
    @pvr.add_import "#{MNT}/#{dir}", Status::COMPLETED
    @pvr.set_has_file dir, true

    @app.cmd_prune

    assert_equal %W[ #{MNT}/#{dir} ], @pvr.imported
    assert_log :info, "deleting leftover files.+mnt=#{MNT}/#{dir}"
  end

  def test_import_success_no_hasFile_grace
    @app.import_grace = 4 * 3600

    dir = "ep03"
    local_dir = @ng.join(dir).tap do |d|
      d.mkdir
      d.join("some file").write "test"
    end
    @pvr.add_ev App::ST_GRABBED, {
      'date' => '2020',
      'data' => {'downloadClient' => "sabnzbd"},
      'downloadId' => 'nzoid03',
      'sourceTitle' => dir,
    }
    @pvr.add_import "#{MNT}/#{dir}", Status::COMPLETED
    @pvr.set_has_file dir, false

    ##
    # Grace NOT expired
    #
    @app.cmd_prune

    assert_equal %W[ #{MNT}/#{dir} ], @pvr.imported
    assert_log :warn, ".*\\bstill present, allowing.+mnt=#{MNT}/#{dir}"

    ##
    # Grace expired
    #
    FileUtils.touch local_dir.glob("*"), mtime: Time.now - 5 * 3600

    @app.cmd_prune

    assert_equal %W[ #{MNT}/#{dir} ] * 2, @pvr.imported
    assert_log :error, ".*\\bdoesn't have files after.+mnt=#{MNT}/#{dir}"
  end

  def test_unpack
    @app.unpack_grace = 2 * 3600

    title = "ep01"
    dir = "_UNPACK_#{title}"
    local_dir = @ng.join(dir).tap do |d|
      d.mkdir
      d.join("some file").write "test"
    end
    @pvr.add_ev App::ST_GRABBED, {
      'date' => '2020',
      'data' => {'downloadClient' => "sabnzbd"},
      'downloadId' => 'nzoid01',
      'sourceTitle' => title,
    }

    ##
    # Grace NOT expired
    #
    @app.cmd_prune

    assert_equal %W[ ], @pvr.imported
    assert_log :debug, ".*\\bunpacking.+mnt=#{MNT}/#{dir}"

    ##
    # Grace expired
    #
    FileUtils.touch local_dir.glob("*"), mtime: Time.now - 3 * 3600
    @pvr.add_import "#{MNT}/#{dir}", Status::COMPLETED
    @pvr.set_has_file title, true

    @app.cmd_prune

    assert_equal %W[ #{MNT}/#{dir} ], @pvr.imported
  end

  private def assert_log(level, pat)
    re = Regexp.new(" *#{Regexp.escape level} +#{pat}", Regexp::IGNORECASE)
    assert_match re, @log_io.string
  end

  class TestPVR
    def initialize
      @events = []
      @imports = {}
      @imported = []
      @has_files = {}
    end

    attr_reader :imported

    def name; "test_pvr" end
    def history; @events end
    def add_ev(type, ev); @events << ev.merge("eventType" => type) end
    def add_import(mnt_dir, st); @imports[mnt_dir.to_s] = st end
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
end
