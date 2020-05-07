DLDir = Struct.new :local, :mnt do
  def self.from_local(local, root:, mnt:)
    new \
      local,
      mnt + local.relative_path_from(root)
  end

  def status
    case
    when empty? then :empty
    when junk? then :junk
    end
  end

  def contents_mtime
    [local, *local_children].flat_map { |f|
      stat = f.stat
      [stat.mtime, stat.ctime]
    }.max
  end

  def size
    Utils::DU.bytes local
  rescue Utils::DU::RaceCondError
  end

  private def empty?
    local.glob("**/*") { return false }
    true
  end

  private def junk?
    local_children.all? do |f|
      f.file? && f.basename.to_s =~ /^\d+-\d+(\.\d+)+$/
    end
  end

  private def local_children
    local.enum_for(:glob, "*")
  end
end
