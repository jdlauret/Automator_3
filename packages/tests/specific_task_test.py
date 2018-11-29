from automator import Automator


if __name__ == '__main__':
    task_id = 807
    loop_count = 0
    app = Automator(test_task_id=task_id, test_loop_count=loop_count)
    app.run_automator()
