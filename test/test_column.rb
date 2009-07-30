require 'test/unit'
require 'libcchdo.rb'

class TestColumn < Test::Unit::TestCase
  def setup
    @column = Column.new "parameter"
  end
  # def teardown; end

  def test_initialization
    assert_equal(@column.parameter, "parameter", 'The column did not initialize to the correct parameter.') # This should compare with a parameter object when we have a db.
    assert_equal(@column.values, [], 'Missing values array.')
    assert_equal(@column.flags_woce, [], 'Missing WOCE flags array.')
    assert_equal(@column.flags_igoss, [], 'Missing IGOSS flags array.')
  end
  
  def test_get
    assert_equal(@column.get(0), nil)
    @column[0] = 1
    assert_equal(@column.get(0), 1)
    assert_equal(@column[0], 1)
    assert_equal(@column.get(1), nil)
    assert_equal(@column[1], nil)
  end
  
  def test_length
    assert_equal(@column.length, 0)
    @column[0] = 1
    assert_equal(@column.length, 1)
    @column[2] = 1
    assert_equal(@column.length, 3)
  end
  
  def test_set
    @column.set(1, 2, 3, 4)
    assert_equal(@column[1], 2)
    assert_equal(@column.flags_woce[1], 3)
    assert_equal(@column.flags_igoss[1], 4)
    assert_equal(@column.length, 2)
  end
  
  def test_flagged_woce
    assert(!@column.flagged_woce?, 'Column has WOCE flags when there should not be.')
    @column[0] = 1
    assert(!@column.flagged_woce?, 'Column has WOCE flags when there should not be.')
    @column.set(0, 1, 2, 3)
    assert(@column.flagged_woce?, 'Column did not have WOCE flags when there should have been.')
  end
  
  def test_flagged_igoss
    assert(!@column.flagged_igoss?, 'Column has IGOSS flags when there should not be.')
    @column[0] = 1
    assert(!@column.flagged_igoss?, 'Column has IGOSS flags when there should not be.')
    @column.set(0, 1, 2, 3)
    assert(@column.flagged_igoss?, 'Column did not have IGOSS flags when there should have been.')
  end
end
