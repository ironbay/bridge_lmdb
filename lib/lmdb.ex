defmodule Bridge.LMDB do
  use Rustler, otp_app: :bridge_lmdb

  def open_env(_arg1), do: error()

  def txn_write_new(_ctx), do: error()

  def txn_write_commit(_txn), do: error()

  def txn_read_new(_env), do: error()

  def txn_read_abort(_txn), do: error()

  def get(_txn, _key), do: error()
  def put(_txn, _key, _value), do: error()
  def delete(_txn, _key), do: error()
  def scan(_env, _min, _max, _take), do: error()

  def test(_env, _min, _max), do: error()
  def range(_txn, _start, _end), do: error()
  def range_next(_cur), do: error()

  def range_take(_cur, _count), do: error()

  def range_abort(_cur), do: error()

  def batch_write(_ctx, _puts, _deletes), do: error()

  def error() do
    :erlang.nif_error(:nif_not_loaded)
  end

  def stream(env, min, max) do
    Stream.resource(
      fn ->
        {min, max}
      end,
      fn {min, max} ->
        case Bridge.LMDB.scan(env, min, max, 100) do
          {:ok, {new_min, results}} ->
            {results, {new_min <> <<0>>, max}}

          _ ->
            {:halt, :done}
        end
      end,
      fn _ -> :ok end
    )
  end

  @max 100_000
  def test_txn() do
    {:ok, env} = open_env("data")
    {:ok, txn} = Bridge.LMDB.txn_write_new(env)

    Enum.map(0..@max, fn item ->
      Bridge.LMDB.put(txn, inspect(item), "foo")
    end)

    Bridge.LMDB.txn_write_commit(txn)
  end

  def test_batch() do
    {:ok, env} = open_env("data")

    Bridge.LMDB.batch_write(env, Enum.map(0..@max, fn item -> {inspect(item), "foo"} end), [])
  end

  def test_scan() do
    {:ok, env} = open_env("data")

    stream(env, "0", "9")
    |> Enum.count()
  end

  def test_test() do
    {:ok, env} = open_env("data")
    test(env, "0", "9")
  end
end

defmodule Benchmark do
  def measure(function) do
    function
    |> :timer.tc()
    |> elem(0)
    |> Kernel./(1_000_000)
  end
end
