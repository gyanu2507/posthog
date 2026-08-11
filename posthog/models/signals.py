from contextlib import contextmanager
from functools import wraps

from django.dispatch import Signal
from django.dispatch.dispatcher import receiver

is_muted = False

# Used for any model that requires an activity log of changes
# To use this, a model must either:
# 1. Include ModelActivityMixin in the model's inheritance
# 2. Or override the save method and call this signal manually
# See FeatureFlag for an example.
# Not every receiver only writes an activity entry: it is also the one signal carrying a full
# `before_update`, so some receivers use it for data integrity that depends on the old value
# (see `products/feature_flags/backend/session_recording_links.py`). Muting a sender through
# `signal_exclusions` disables those too.
model_activity_signal = Signal()

# Sent by Team.rotate_secret_token_and_save after the new token is persisted, so
# dependent stores (e.g. the conversations signing secret) can stay in sync without
# core importing product models. Receives `team` with the new token already saved.
secret_api_token_rotated = Signal()


def mutable_receiver(*args, **kwargs):
    """
    Decorator for a django signal handler which can be turned off during mass deletes.
    """

    def _inner(handler):
        @receiver(*args, **kwargs)
        @wraps(handler)
        def new_handler(*f_args, **f_kwargs):
            if not is_muted:
                handler(*f_args, **f_kwargs)

        return new_handler

    return _inner


@contextmanager
def mute_selected_signals():
    """
    Code in this block does not call _any_ of the receive hooks set up with @mutable_receiver.

    This can be useful for mass object deletion scenarios, where a given hook might be called thousands of times.
    """

    global is_muted
    try:
        is_muted = True
        yield
    finally:
        is_muted = False
