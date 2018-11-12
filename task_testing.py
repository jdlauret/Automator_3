from automator import Automator

if __name__ == '__main__':
    task_id = 7
    app = Automator(test_task_id=task_id)
    app.run_automator()