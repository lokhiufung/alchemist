from alchemist.constants.messages import EVENT_TYPES
from alchemist.messages import make_event


def test_event_types_closed_set():
    assert EVENT_TYPES == ("tick_update", "bar_update", "order_update", "position_update",
                           "balance_update", "repaired_fill", "book_op", "end_of_data")


def test_make_event_carries_enqueue_time():
    event = make_event("tick_update", {"price": 1.0}, 12.5)
    assert event == {"event_type": "tick_update", "data": {"price": 1.0}, "enqueued_at": 12.5}
