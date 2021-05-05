import functools
import inspect
import json
import os
import warnings


# State numbers:
#
#  0: `state` is None. The stream has no more elements. `step` can't be called.
#
#  1: `state` is a function. Calling `step` with a 2-tuple of positional and
#     keyword arguments passes those arguments to the function, and then
#     transitions to state 0 or 2 depending on whether the function is a
#     generator. It returns the first value yielded by the generator, or
#     raises `StopIteration` with a payload if the function or generator
#     returns.
#
#  2: `state` is the iterator returned by the generator function. Calling `step`
#     returns the next value yielded by the generator (called with `param`)
#     or, if the generator returns, raises `StopIteration` with the returned
#     value and transitions to state 0.
#
# This is essentially a tool for getting plain functions to behave like
# generators that return a value immediately without yielding anything.

class _Stream:
    def __init__(self, f):
        self.state_no = 1
        self.state = f

    def _finish(self):
        self.state_no = 0
        self.state = None

    def step(self, param):
        if self.state_no == 1:
            f = self.state
            (args, kwargs) = param
            res = f(*args, **kwargs)
            if inspect.isgeneratorfunction(f):
                self.state_no = 2
                self.state = res
                return next(res)
            else:
                self._finish()
                raise StopIteration(res)

        elif self.state_no == 2:
            try:
                return self.state.send(param)
            except StopIteration:
                self._finish()
                raise

        else:
            assert False, "called step from invalid state"


class _Promise:
    def __init__(self, src, tracked_id, do_compute, evict_key, f, f_args, f_kwargs):
        self.resolved = False
        assert isinstance(src, str)
        assert isinstance(tracked_id, int)
        assert isinstance(do_compute, bool)
        assert isinstance(evict_key, str)
        self.src = src
        self.tracked_id = tracked_id
        self._do_compute = do_compute
        self._evict_key = evict_key
        self._stream_param = (f_args, f_kwargs)
        self._cache_args = (f_args, tuple(sorted(f_kwargs.items())))
        self._stream = _Stream(f)

    def _resolve(self):
        self.resolved = True
        self._stream_param = None
        self._cache_args = None
        self._stream = None

    def step(self, cache):
        assert not self.resolved
        if self._cache_args:
            try:
                res = cache.lookup(self.src, *self._cache_args, self._evict_key)
                self._resolve()
                return res
            except KeyError:
                pass
        assert self._do_compute, \
            "computation of function '{}' was disabled, " \
            "but it was not found in the cache" .format(self.src)
        # The value of `resolved` after calling this function determines
        # whether the value returned by this function was yielded or returned
        # by the `_stream`. If the former, the caller should supply a new value
        # to pass to the stream next step by overwriting `input_for_next`.
        try:
            res = self._stream.step(self._stream_param)
            return res
        except StopIteration as e:
            if len(e.args) == 0:
                res = None
            else:
                res = e.args[0]
            cache.insert(self.src, *self._cache_args, res, self._evict_key)
            self._resolve()
            return res

    def supply_input(self, x):
        self._stream_param = x

    def __del__(self):
        if not self.resolved:
            warnings.warn(
                "promise was never resolved - did you forget to yield it?",
                ResourceWarning
            )


class _Cache:
    def __init__(self, names, to_json_fn):
        self._names = names
        self._names_to_ids = {}
        self._to_json_fn = to_json_fn

    def empty(to_json_fn):
        return _Cache({}, to_json_fn)

    def from_json(json_value, *, from_json_fn, to_json_fn):
        return _Cache(
            {
                name: _NameSubcache.from_json(
                    subcache_json,
                    name=name,
                    from_json_fn=from_json_fn,
                ) for name, subcache_json in json_value["names"].items()
            },
            to_json_fn,
        )

    def to_json(self):
        return {"names": {
            name: subcache.to_json(
                name=name,
                to_json_fn=self._to_json_fn
            )
            for name, subcache in self._names.items()
            if not subcache.is_empty()
        }}

    def lookup(self, name, args, kwargs, evict_key=None):
        assert isinstance(kwargs, tuple)
        return self._names[name].lookup(args, kwargs, evict_key)

    def _get_subcache(self, name):
        if name not in self._names:
            self._names[name] = _NameSubcache.empty()
        return self._names[name]

    def insert(self, name, args, kwargs, res, evict_key=None):
        assert isinstance(kwargs, tuple)
        self._get_subcache(name).insert(args, kwargs, res, evict_key)


class _NameSubcache:
    def __init__(self, entries, evict_key):
        self.entries = entries
        self._evict_key = evict_key

    def empty():
        return _NameSubcache({}, None)

    def is_empty(self):
        return len(self.entries) == 0 and not self._evict_key

    def from_json(json_value, *, name, from_json_fn):
        entries = {}
        for entry_json in json_value["entries"]:
            entry = _Entry.from_json(
                entry_json,
                name=name,
                from_json_fn=from_json_fn
            )
            k = (entry.args, entry.kwargs)
            assert k not in entries, \
                "duplicate cache entries for '{}': args={}, kwargs={}" \
                    .format(name, k[0], k[1])
            entries[k] = entry.res
        evict_key = json_value.get("ev", "")
        return _NameSubcache(entries, evict_key)

    def to_json(self, *, name, to_json_fn):
        res = {"entries": [
            _Entry(args, kwargs, res).to_json(name=name, to_json_fn=to_json_fn)
            for (args, kwargs), res in self.entries.items()
        ]}
        if self._evict_key:
            res["ev"] = self._evict_key
        return res

    def _check_evict(self, evict_key):
        if self._evict_key is None:
            self._evict_Key = evict_key
        elif not (evict_key is None or evict_key == self._evict_key):
            self._evict_key = evict_key
            self.entries = {}

    def lookup(self, args, kwargs, evict_key=None):
        assert isinstance(kwargs, tuple)
        self._check_evict(evict_key)
        return self.entries[(args, kwargs)]

    def insert(self, args, kwargs, res, evict_key=None):
        assert isinstance(kwargs, tuple)
        self._check_evict(evict_key)
        k = (args, kwargs)
        assert k not in self.entries, "value {} is already in cache".format(k)
        self.entries[k] = res


# Represents data associated with a specific cache entry. Note that these
# objects are not actually used to store the cache entries in memory (since
# we need to store the inputs as dictionary keys); it's just an intermediate
# data structure used during JSON encoding/decoding.
class _Entry:
    def __init__(self, args, kwargs, res):
        assert isinstance(args, tuple)
        assert isinstance(kwargs, tuple)
        self.args = args
        self.kwargs = kwargs
        self.res = res

    def from_json(json_value, *, name, from_json_fn):
        *args_json, kwargs_json, res_json = json_value
        args = tuple(
            from_json_fn(arg_json, name=name, pos="arg")
            for arg_json in args_json
        )
        kwargs = tuple(sorted(
            (k, from_json_fn(arg_json, name=name, pos="kwarg"))
            for k, arg_json in kwargs_json.items()
        ))
        res = from_json_fn(res_json, name=name, pos="res")
        return _Entry(args, kwargs, res)

    def to_json(self, *, name, to_json_fn):
        return [
            *(to_json_fn(arg, name=name, pos="arg") for arg in self.args),
            {
                k: to_json_fn(arg, name=name, pos="kwarg")
                for k, arg in self.kwargs
            },
            to_json_fn(self.res, name=name, pos="res")
        ]


class Context:
    def __init__(self, cache, *, serialize_fn):
        assert isinstance(cache, _Cache)
        assert callable(serialize_fn)
        self._cache = cache
        self._names_to_ids = {}
        self._serialize_fn = serialize_fn

    def compute(self, promise):
        assert isinstance(promise, _Promise)
        blocked_promises = []
        current_promise = promise
        while True:
            # Check that we aren't calling multiple functions that are tracked
            # using the same name.
            name = current_promise.src
            if name in self._names_to_ids:
                assert self._names_to_ids[name] == current_promise.tracked_id, \
                    "multiple functions are tracked under the name '{}'" \
                        .format(name)
            else:
                self._names_to_ids[name] = current_promise.tracked_id

            result = current_promise.step(self._cache)

            # If the current promise is not yet resolved, then it is blocked on
            # the promise it yielded. This means we should add it to the stack
            # of blocked promises and focus on resolving the one it yielded.
            if not current_promise.resolved:
                # TODO: these should just act as normal generators
                assert isinstance(result, _Promise), \
                    "tracked function yielded something other than a promise"
                blocked_promises.append(current_promise)
                current_promise = result

            # If the promise did resolve, and it wasn't the root, then its
            # result should be supplied as input to the promise that called it.
            elif len(blocked_promises) > 0:
                current_promise = blocked_promises.pop()
                current_promise.supply_input(result)

            # When the root promise resolves, its result is the result of the
            # final computation.
            else:
                return result

    def _sync(self):
        self._serialize_fn(self._cache.to_json())

    def __enter__(self):
        return self

    def __exit__(self, _exc_ty, _exc, _traceback):
        self._sync()


_num_tracked = 0
def _track(name=None, compute=True, evict_key=""):
    def decorate(f):
        global _num_tracked
        tracked_id = _num_tracked
        _num_tracked += 1
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            src = f.__name__ if name is None else name
            return _Promise(src, tracked_id, compute, evict_key, f, args, kwargs)
        return wrapper
    return decorate


def track(*args, **kwargs):
    # A special case that makes this function directly usable as a decorator.
    if len(args) == 1 and len(kwargs) == 0:
        f = args[0]
        assert callable(f)
        return _track(name=f.__name__)(f)
    else:
        return _track(*args, **kwargs)


def open(filename="_inax.json"):
    # since we're introducing a new function called `open`
    from builtins import open as file_open

    def to_json_fn(value, name, pos):
        # assume the value is valid JSON
        return value

    def from_json_fn(value, name, pos):
        # assume the JSON is a valid value
        return value

    try:
        f = file_open(filename, "r")
    except FileNotFoundError:
        cache = _Cache.empty(to_json_fn)
    else:
        cache = _Cache.from_json(
            json.load(f),
            from_json_fn=from_json_fn,
            to_json_fn=to_json_fn
        )
        f.close()

    # in case the cwd has changed when serialization happens
    path = os.path.abspath(filename)
    def serialize_fn(cache):
        with file_open(path, "w") as f:
            json.dump(cache, f, separators=",:")
            f.write('\n')

    return Context(cache, serialize_fn=serialize_fn)
