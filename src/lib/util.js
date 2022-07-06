'use strict'

const config = require('../config')

/* c8 ignore next 3 */
function now() {
  return config.now || new Date().toISOString()
}

/**
 * execute tasks in parallel with concurrency
 * starts on the first "add"
 * run "concurrency" tasks at time
 * calls "onTaskComplete" after each task, passing the task's output
 * stops at first error
 * does not throw error, return it on "done"
 */
function queuedTasks({ concurrency = 1, onTaskComplete } = {}) {
  const _queue = []
  const taskCompleted = []
  let running = 0
  let error

  let _resolve

  function _end () {
    if (onTaskComplete) {
      Promise.allSettled(taskCompleted).then(() => {
        _resolve && _resolve({ error })
      })
      return
    }
    _resolve && _resolve({ error })
  }

  function _done() {
    if (error) {
      _end()
      return
    }
    if (_queue.length > 0) {
      run()
      return
    }
    if (running === 0) {
      _end()
    }
  }

  function add(f) {
    if (error) { return }
    _queue.push(f)
    run()
  }

  async function run() {
    if (running >= concurrency) {
      return
    }
    const f = _queue.shift()
    if (!f) {
      _done()
      return
    }
    running++
    try {
      const result = await f()
      onTaskComplete && onTaskComplete(result)
    } catch (err) {
      error = err
    } finally {
      running--
      _done()
    }
  }

  function done() {
    return new Promise(resolve => {
      _resolve = resolve
      _done()
    })
  }

  return { add, run, done }
}

module.exports = {
  now,
  queuedTasks
}
