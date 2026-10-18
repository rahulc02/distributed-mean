from dmean.supervisor.reconcile import Plan, WorkerView, reconcile, spawn_delay


def w(id, status="RUNNING", busy=False, started=0.0):
    return WorkerView(id, status, busy, started)


def test_spawns_up_to_desired():
    assert reconcile(3, [w("a")]) == Plan(spawn=2, drain=())


def test_draining_workers_do_not_count_toward_desired():
    assert reconcile(2, [w("a"), w("b", status="DRAINING")]) == Plan(spawn=1, drain=())


def test_drains_idle_workers_first_then_the_youngest():
    workers = [w("old-busy", busy=True, started=1), w("young-idle", started=9),
               w("old-idle", started=2), w("young-busy", busy=True, started=8)]
    assert reconcile(1, workers).drain == ("young-idle", "old-idle", "young-busy")


def test_steady_state_is_a_no_op():
    assert reconcile(2, [w("a"), w("b", status="STARTING")]) == Plan(spawn=0, drain=())


def test_zero_pauses_everything():
    assert set(reconcile(0, [w("a"), w("b")]).drain) == {"a", "b"}


def test_backoff_doubles_and_caps():
    assert [spawn_delay(n) for n in range(5)] == [0.0, 1.0, 2.0, 4.0, 8.0]
    assert spawn_delay(20) == 60.0
