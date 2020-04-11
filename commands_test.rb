$:.unshift __dir__
require 'minitest/autorun'
require 'main'

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
end
