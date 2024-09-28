from config import SKIP_STEPS


def next_step(step: int=None, stop: bool=False):
    if SKIP_STEPS is False and stop is True:
        if step:
            current_step = step - 1
            result = input(f"Continue to Step {step}? y/n: ")
            if result != "y":
                raise KeyboardInterrupt(f"scrape_the_law program stopped at Step {current_step}.")
        else:
            result = input(f"Continue next step? y/n: ")
            if result != "y":
                raise KeyboardInterrupt(f"scrape_the_law program stopped at step.")
    else:
        return
