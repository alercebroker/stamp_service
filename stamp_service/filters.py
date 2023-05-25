from ralidator_fastapi.get_ralidator import get_ralidator


def filter_atlas_data(filter_name, arg_key):
    def wrapper_function(arg_function):
        def decorator_function(*args, **kwargs):
            """
            The decorator function works because we assume that the
            survey id exists.
            """
            ralidator = get_ralidator(kwargs["request"])
            apply_all = "*" in ralidator.user_filters
            if (apply_all or filter_name in ralidator.user_filters) and kwargs[
                arg_key
            ] == "atlas":
                return None
            else:
                return arg_function(*args, **kwargs)

        return decorator_function

    return wrapper_function
