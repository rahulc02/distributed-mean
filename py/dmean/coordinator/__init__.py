"""The coordinator: the only component that changes task and operand state.

Layout (functional core, imperative shell):
    model.py      plain data types used by the core
    keys.py       every naming convention, in one place
    grouping.py   pure: which READY operands become the next task
    planning.py   pure: leaf tasks, merge tasks, exact task and operand totals
    ports.py      the interfaces the service depends on
    service.py    the logic: start jobs, handle completions, recover
    pg.py         Postgres implementation of the storage port
    rabbit.py     RabbitMQ publisher and message conversion
    main.py       process shell: leadership, consumers, loops
"""
