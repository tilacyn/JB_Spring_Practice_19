package dijkstra

import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import kotlin.Comparator
import kotlin.concurrent.thread

val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance.get(), o2!!.distance.get()) }

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathSequential(start: Node, destination: Node): Int {
    start.distance.set(0)
    val q = PriorityQueue<Node>(NODE_DISTANCE_COMPARATOR)
    q.add(start)
    while (q.isNotEmpty()) {
        val cur = q.poll()
        for (e in cur.outgoingEdges) {
            if (e.to.distance.get() > cur.distance.get() + e.weight) {
                e.to.distance.set(cur.distance.get() + e.weight)
                q.remove(e.to) // inefficient, but used for tests only
                q.add(e.to)
            }
        }
    }
    return destination.distance.get()
}

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node, destination: Node): Int {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance.set(0)
    // Create a priority (by distance) queue and add the start node into it
    val q = PriorityQueue(workers, NODE_DISTANCE_COMPARATOR)
    q.add(start)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val working = AtomicInteger(workers)
    repeat(workers) {
        thread {
            var waiting = false
            var relaxedVertex = 0
            while (true) {
                val cur: Node? = synchronized(q) { q.poll() }
                if (cur == null) {
                    if (!waiting) {
                        waiting = true
                        working.getAndDecrement()
                    }
                    if (working.get() == 0) {
                        break
                    }
                    continue
                }
                if (waiting) {
                    waiting = false
                    working.getAndIncrement()
                }

                for (e in cur.outgoingEdges) {
                    var distance = e.to.distance.get()
                    while (distance > cur.distance.get() + e.weight) {
                        if (e.to.distance.compareAndSet(distance, cur.distance.get() + e.weight)) {
                            relaxedVertex++
                            synchronized(q) {
                                q.add(e.to)
                            }
                            break
                        }
                        distance = e.to.distance.get()
                    }
                }
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
    // Return the result
    return destination.distance.get()
}