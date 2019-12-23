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
  def range(_txn, _start, _end), do: error()
  def range_next(_cur), do: error()

  def range_take(_cur, _count), do: error()

  def range_abort(_cur), do: error()

  def error() do
    :erlang.nif_error(:nif_not_loaded)
  end

  def test() do
    {:ok, env} = open_env("data")
    {:ok, rtx} = txn_read_new(env)

    start = :os.system_time(:millisecond)

    Stream.resource(
      fn ->
        {"audit:log", "audit:loh"}
      end,
      fn {min, max} ->
        case scan(env, min, max, 100) do
          {:ok, {new_min, results}} ->
            {prefix, [head | _]} =
              new_min
              |> String.to_charlist()
              |> Enum.split(String.length(new_min) - 1)

            {results, {String.Chars.to_string(prefix ++ [head + 1]), max}}

          _ ->
            {:halt, :done}
        end
      end,
      fn
        _ -> :ok
      end
    )
    |> Enum.count()
    |> IO.inspect()

    IO.inspect(:os.system_time(:millisecond) - start)

    txn_read_abort(rtx)
  end
end
