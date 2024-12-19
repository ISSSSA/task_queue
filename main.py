from datetime import datetime
import random
import aiosqlite

from asyncio import Semaphore, sleep

from config import DB_PATH, MAX_CONCURRENT_TASKS
from tasks.models import TaskStatus

from fastapi import FastAPI, HTTPException, BackgroundTasks

app = FastAPI()
semaphore = Semaphore(MAX_CONCURRENT_TASKS)


@app.on_event("startup")
async def startup_event():
    global connection
    global cursor
    connection = await aiosqlite.connect(DB_PATH, check_same_thread=False)
    cursor = await connection.cursor()
    await cursor.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        create_time TEXT,
        start_time TEXT,
        exec_time REAL,
        status TEXT
    )
    """)
    await connection.commit()


@app.post("/add_task")
async def add_task(background_tasks: BackgroundTasks):
    create_time = datetime.utcnow().isoformat()
    await cursor.execute("INSERT INTO tasks (create_time, status) VALUES (?, ?)", (create_time, "In Queue"))
    task_id = cursor.lastrowid
    await connection.commit()
    background_tasks.add_task(execute_task, task_id)
    return {"task_id": task_id}


@app.get("/task_status/{task_id}", response_model=TaskStatus)
async def task_status(task_id: int):
    await cursor.execute("SELECT status, create_time, start_time, exec_time FROM tasks WHERE id = ?", (task_id,))
    task = cursor.fetchone()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskStatus(
        status=task[0],
        create_time=task[1],
        start_time=task[2],
        time_to_execute=task[3],
    )


@app.on_event("shutdown")
async def shutdown_event():
    await connection.close()


async def execute_task(task_id: int):
    async with semaphore:
        start_time = datetime.utcnow().isoformat()
        await cursor.execute("UPDATE tasks SET status = ?, start_time = ? WHERE id = ?", ("Run", start_time, task_id))
        await connection.commit()

        exec_time = random.randint(0, 10)
        await sleep(exec_time)

        await cursor.execute(
            "UPDATE tasks SET status = ?, exec_time = ? WHERE id = ?",
            ("Completed", exec_time, task_id),
        )
        await connection.commit()
