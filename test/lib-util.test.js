'use strict'

const t = require('tap')

const { queuedTasks } = require('../src/lib/util')

t.test('queuedTasks', async t => {
  t.test('should run a task and complete it', async t => {
    const q = queuedTasks()

    let task
    q.add(async () => { task = 1 })
    await q.done()

    t.equal(task, 1)
  })

  t.test('should run some tasks and complete them', async t => {
    const q = queuedTasks()

    const tasks = []
    const results = []

    for (let i = 0; i < 50; i++) {
      q.add(async () => { tasks.push(i) })
      results.push(i)
    }
    await q.done()

    t.same(tasks, results)
  })

  t.test('should run some tasks with concurrency and complete them', async t => {
    const q = queuedTasks({ concurrency: 5 })

    const tasks = []
    const results = []

    for (let i = 0; i < 50; i++) {
      q.add(async () => { tasks.push(i) })
      results.push(i)
    }
    await q.done()

    t.same(tasks, results)
  })

  t.test('should run some tasks with different execution time with concurrency and complete them', async t => {
    const q = queuedTasks({ concurrency: 3 })

    const tasks = []
    const results = []

    for (let i = 0; i < 8; i++) {
      q.add(() => {
        return new Promise((resolve) => {
          setTimeout(() => {
            tasks.push(i)
            resolve()
          }, 50 * i)
        })
      })
      results.push(i)
    }

    await q.done()

    t.same(tasks, results)
  })

  t.test('should run some tasks and call onTaskComplete properly', async t => {
    const tasks = []
    const onTaskComplete = function (out) {
      tasks.push(out)
    }
    const q = queuedTasks({ concurrency: 4, onTaskComplete })

    const results = []

    for (let i = 0; i < 4; i++) {
      q.add(() => { return i })
      results.push(i)
    }

    await q.done()

    t.same(tasks, results)
  })

  t.test('should ends on first task error', async t => {
    const q = queuedTasks()

    let done

    for (let i = 0; i < 8; i++) {
      q.add(() => {
        return new Promise((resolve, reject) => {
          done = i
          if (i === 4) { reject(new Error('BOOM')) } else { resolve() }
        })
      })
    }

    const result = await q.done()

    t.equal(done, 4)
    t.same(result.error, new Error('BOOM'))
  })

  t.test('should ends on first task error in an async loop', async t => {
    const q = queuedTasks({ concurrency: 1 })

    const iterable = {
      [Symbol.asyncIterator]() {
        let i = 0
        return {
          next() {
            const done = i === 16
            const value = done ? undefined : i++
            return Promise.resolve({ value, done })
          }
        }
      }
    }

    let done
    for await (const i of iterable) {
      q.add(async () => {
        done = i
        if (i === 4) { throw new Error('BOOM') }
      })
    }

    const result = await q.done()

    t.equal(done, 4)
    t.same(result.error, new Error('BOOM'))
  })

  t.test('should ends on first task error accordingly to concurrency', async t => {
    const q = queuedTasks({ concurrency: 3 })

    let done

    for (let i = 0; i < 16; i++) {
      q.add(() => {
        return new Promise((resolve, reject) => {
          done = i
          if (i === 4) { reject(new Error('BOOM')) } else { resolve() }
        })
      })
    }

    const result = await q.done()

    // must end at the second set of concurrent tasks
    t.ok(done <= 6)
    t.same(result.error, new Error('BOOM'))
  })

  t.test('should handle calling "run" on an empty queue', async t => {
    const q = queuedTasks()
    q.run()
    await q.done()

    t.pass()
  })

  t.test('should handle calling "done" on an empty queue', async t => {
    const q = queuedTasks()
    await q.done()

    t.pass()
  })
})
