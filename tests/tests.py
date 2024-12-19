import sqlite3

from fastapi.testclient import TestClient
from unittest import TestCase
from main import app
import unittest

connect = sqlite3.connect('tasks.db')
cursor = connect.cursor()


def setup_test_db():
    cursor.execute("DROP TABLE IF EXISTS tasks")
    cursor.execute(
        """
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            create_time TEXT,
            start_time TEXT,
            exec_time REAL,
            status TEXT
        )
        """
    )
    connect.commit()


class TestTaskQueue(TestCase):
    def setUp(self):
        setup_test_db()

    def test_add_task(self):
        with TestClient(app) as client:
            response = client.post("/add_task")
            self.assertEqual(response.status_code, 200)
            task_id = response.json().get("task_id")
            self.assertIsInstance(task_id, int)

            cursor.execute("SELECT id, status FROM tasks WHERE id = ?", (task_id,))
            task = cursor.fetchone()
            self.assertIsNotNone(task)
            self.assertEqual(task[1], "Completed")


if __name__ == "__main__":
    unittest.main()
