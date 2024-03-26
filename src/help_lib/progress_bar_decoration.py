import sys


def progress_bar(func):
    def wrapper(self, lst, output):
        total = len(lst)
        progress_length = 50

        for i, item in enumerate(lst):
            func(self, item)

            progress = (i + 1) / total
            filled_length = int(progress_length * progress)
            empty_length = progress_length - filled_length
            if progress == 1:
                end = '\U0000EE05'
            else:
                end = '\U0000EE02'
            progress_bar_str = '\U0000EE03' + '\U0000EE04' * filled_length + '\U0000EE01' * empty_length + end
            sys.stdout.write('\r%s %s %.2f%%' % (output, progress_bar_str, progress * 100))
            sys.stdout.flush()
        print()

    return wrapper
