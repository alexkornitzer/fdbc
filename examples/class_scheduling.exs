alias FDBC.Database
alias FDBC.Directory
alias FDBC.Network
alias FDBC.Subspace
alias FDBC.Transaction
alias FDBC.Tuple

:ok = FDBC.api_version(730)
:ok = Network.start()

db = Database.create(nil, transaction_timeout: 60_000, transaction_retry_limit: 100)
scheduling = FDBC.transact(db, fn tr ->
  Directory.new() |> Directory.open!(tr, ["scheduling"], create: true)
end)
attends = Directory.subspace(scheduling) |> Subspace.concat(["attends"])
course = Directory.subspace(scheduling) |> Subspace.concat(["class"])

levels = ["intro", "for dummies", "remedial", "101", "201", "301", "mastery", "lab", "seminar"]
types = ["chem", "bio", "cs", "geometry", "calc","alg", "film", "music", "art", "dance"]
times = Enum.map(2..19, fn x -> "#{x}:00" end)
class_names = for x <- times, y <- types, z <- levels, do: "#{x} #{y} #{z}"

defmodule ClassScheduling do
  def init(tr, scheduling, course, class_names) do
    {start, stop} = Directory.subspace(scheduling) |> Subspace.range()
    Transaction.clear_range(tr, start, stop)
    Enum.each(class_names, fn class -> add_class(tr, course, class) end)
  end

  defp add_class(tr, course, class) do
    key = Subspace.pack(course, [class])
    value = Tuple.pack([100])
    Transaction.set(tr, key, value)
  end

  def available_classes(tr, course) do
    {start, stop} = Subspace.range(course)
    kvs = Transaction.get_range(tr,start,stop)
    for {k, v} <- kvs, Tuple.unpack(v) != 0, do: Subspace.unpack(course, k) |> List.first()
  end

  def signup(tr, attends, course, sid, class) do
    rec = Subspace.pack(attends, [sid, class])
    unless Transaction.get(tr, rec) do
      [seats_left] = Transaction.get(tr, Subspace.pack(course, [class])) |> Tuple.unpack()
      if seats_left == 0, do: raise "no remaining seats"

      {start, stop} = Subspace.range(attends, [sid])
      classes = Transaction.get_range(tr, start, stop)
      if length(classes) == 5, do: raise "too many classes"

      Transaction.set(tr, Subspace.pack(course, [class]), Tuple.pack([seats_left - 1]))
      Transaction.set(tr, rec, <<>>)
    end
    :ok
  end

  def drop(tr, attends, course, sid, class) do
    rec = Subspace.pack(attends, [sid, class])
    if Transaction.get(tr, rec) do
      [seats_left] = Transaction.get(tr, Subspace.pack(course, [class])) |> Tuple.unpack()
      Transaction.set(tr, Subspace.pack(course, [class]), Tuple.pack([seats_left + 1]))
      Transaction.clear(tr, rec)
    end
    :ok
  end

  def switch(tr, attends, course, sid, old, new) do
    drop(tr, attends, course, sid, old)
    signup(tr, attends, course, sid, new)
  end
end

defmodule Student do
  def indecisive(db, attends, course, classes, i, ops) do
    sid = "s#{i}"
    do_indecisive(db, attends, course, classes, sid, ops, [])
  end

  defp do_indecisive(_, _, _, _, _, 0, _), do: :ok

  defp do_indecisive(db, attends, course, classes, sid, ops, my_classes) do
    class_len = length(my_classes)
    moods = if class_len > 0 do
      ["drop", "switch"]
    else
      []
    end
    moods = if class_len < 5 do
      moods ++ ["add"]
    else
      moods
    end

    {classes, my_classes} = try do
      classes = if classes == [] do
        FDBC.transact(db, fn tr ->
          ClassScheduling.available_classes(tr, course)
        end)
      else
        classes
      end

      case Enum.random(moods) do
        "add" ->
          class = Enum.random(classes)
          FDBC.transact(db, fn tr ->
            ClassScheduling.signup(tr, attends, course, sid, class)
          end)
          {classes, my_classes ++ [class]}
        "drop" ->
          class = Enum.random(my_classes)
          FDBC.transact(db, fn tr ->
            ClassScheduling.drop(tr, attends, course, sid, class)
          end)
          {classes, List.delete(my_classes, class)}
        "switch" ->
          old_class = Enum.random(my_classes)
          new_class = Enum.random(classes)
          FDBC.transact(db, fn tr ->
            ClassScheduling.switch(tr, attends, course, sid, old_class, new_class)
          end)
          {classes, List.delete(my_classes, old_class) ++ [new_class]}
        _ -> {classes, my_classes}
      end
    rescue
      e ->
        IO.inspect(e)
        IO.puts("need to recheck available classes")
        {[], my_classes}
    end

    do_indecisive(db, attends, course, classes, sid, ops - 1, my_classes)
  end
end

FDBC.transact(db, fn tr ->
  ClassScheduling.init(tr, scheduling, course, class_names)
end)
IO.puts("initialised")
ops = 10
tasks = for i <- 0..9 do
  Task.async(fn -> Student.indecisive(db, attends, course, class_names, i, ops) end)
end
for t <- tasks, do: Task.await(t)
IO.puts("ran #{length(tasks) * ops} transactions")
FDBC.transact(db, fn tr ->
  for s <- 0..9 do
    sid = "s#{s}"
    student_attends = Subspace.concat(attends, [sid])
    {start, stop} = Subspace.range(student_attends)
    classes = Transaction.get_range(tr, start, stop) |> Enum.map(fn {k, _} ->
      Subspace.unpack(student_attends, k) |> List.first()
    end)
    IO.puts("student '#{sid}' will attend '#{Enum.join(classes, ", ")}'")
  end
end)
