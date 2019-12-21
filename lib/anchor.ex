defmodule Anchor do
  use Rustler, otp_app: :anchor

  def open_env(_arg1), do: error()

  def txn_write_new(_ctx), do: error()

  def txn_write_commit(_txn), do: error()

  def txn_read_new(_env), do: error()

  def txn_read_abort(_txn), do: error()

  def get(_txn, _key), do: error()
  def put(_txn, _key, _value), do: error()
  def range(_txn, _start, _end), do: error()
  def range_next(_cur), do: error()

  def range_abort(_cur), do: error()

  def error() do
    :erlang.nif_error(:nif_not_loaded)
  end

  def test() do
    {:ok, env} = Anchor.open_env("data")
    {:ok, rtx} = Anchor.txn_read_new(env)

    Stream.resource(
      fn ->
        {:ok, cur} = Anchor.range(rtx, "audit:log", "audit:loh")
        cur
      end,
      fn cur ->
        case range_next(cur) do
          {:ok, kv} ->
            {[kv], cur}

          _ ->
            {:halt, :done}
        end
      end,
      fn
        :done -> :ok
        cur -> range_abort(cur)
      end
    )
    |> Enum.take(100)
    |> IO.inspect()

    Anchor.txn_read_abort(rtx)
  end
end
