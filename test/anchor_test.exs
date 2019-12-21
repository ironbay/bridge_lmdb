defmodule AnchorTest do
  use ExUnit.Case
  doctest Anchor

  test "greets the world" do
    assert Anchor.hello() == :world
  end
end
