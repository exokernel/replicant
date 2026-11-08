// Writes the crdt-benchmarks inputs as trace files, and writes the spec files.
//
// This script runs the unmodified js-lib of crdt-benchmarks against a factory
// that records each call. The recorded calls are what every library received.
// The inputs depend on run order, because one PRNG serves the whole suite and
// b2.js draws from it at import time. So this script runs the whole suite, in
// suite order.
//
// Usage: node dump.mjs <crdt-benchmarks clone> <editing-traces clone> <out dir> <repo dir>

import { createHash } from 'node:crypto'
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { gunzipSync } from 'node:zlib'

const [clone, editingTraces, outDir, repoDir] = process.argv.slice(2)
const info = console.info
console.info = () => {}
const log = console.log
console.log = () => {}
const lib = await import(`${clone}/js-lib/index.js`)
console.log = log

mkdirSync(join(outDir, 'crdt-benchmarks'), { recursive: true })
mkdirSync(join(outDir, 'editing-traces'), { recursive: true })

/** The benchmark that the suite runs now. */
let current = null
/** The recorded state of each benchmark, by name. */
const benches = new Map()
const bench = () => {
  if (!benches.has(current)) benches.set(current, { nextDoc: 0, txns: [], agents: new Map(), last: null })
  return benches.get(current)
}

/**
 * A plain document that records each transaction. It has no merge rule. After
 * a concurrent update it stops tracking content, and it reports a sorted list
 * of all ops so that the suite's own checks still pass.
 */
class Recorder {
  constructor (handler, id) {
    this.handler = handler
    this.id = id
    this.text = ''
    this.list = []
    this.map = {}
    this.depth = 0
    this.pending = []
    // The transactions that this doc has, as a set of causal heads.
    this.heads = new Set()
    this.diverged = false
    this.bag = []
  }

  /** Runs one edit and buffers it for the open transaction. */
  edit (kind, args) {
    this.pending.push({ kind, args })
    this.bag.push(kind + JSON.stringify(args))
    this.applyPlain(kind, args)
    if (this.depth === 0) this.flush()
  }

  /** Closes the open transaction, and gives its update to the handler. */
  flush () {
    if (this.pending.length === 0) return
    const b = bench()
    if (!b.agents.has(this.id)) b.agents.set(this.id, b.agents.size)
    const index = b.txns.length
    const txn = { agent: b.agents.get(this.id), parents: [...this.heads].sort((x, y) => x - y) }
    for (const { kind, args } of this.pending) {
      const field = { text: 'patches', list: 'listOps', map: 'mapOps' }[kind]
      ;(txn[field] ??= []).push(args)
    }
    b.txns.push(txn)
    this.heads = new Set([index])
    const update = { index, parents: txn.parents, ops: this.pending, from: this.id, length: 1 }
    this.pending = []
    this.handler(update)
  }

  /** Applies an update from another doc. */
  applyUpdate (update) {
    if (Buffer.isBuffer(update)) {
      Object.assign(this, JSON.parse(update.toString()))
      return
    }
    if (update.from === this.id) return
    // The update is concurrent if this doc has a head that it does not follow.
    const concurrent = [...this.heads].some(h => !update.parents.includes(h))
    if (concurrent) this.diverged = true
    for (const { kind, args } of update.ops) {
      this.bag.push(kind + JSON.stringify(args))
      if (!this.diverged) this.applyPlain(kind, args)
    }
    for (const p of update.parents) this.heads.delete(p)
    this.heads.add(update.index)
  }

  /** Applies one op to the plain content. */
  applyPlain (kind, args) {
    if (kind === 'text') {
      const [p, d, s] = args
      if (p + d > this.text.length) throw new Error(`position ${p}+${d} past ${this.text.length}`)
      this.text = this.text.slice(0, p) + s + this.text.slice(p + d)
    } else if (kind === 'list') {
      const [p, d, v] = args
      this.list.splice(p, d, ...v)
    } else {
      this.map[args[0]] = args[1]
    }
  }

  // The factory interface of crdt-benchmarks.
  insertText (i, s) { this.edit('text', [i, 0, s]) }
  deleteText (i, n) { this.edit('text', [i, n, '']) }
  insertArray (i, v) { this.edit('list', [i, 0, v]) }
  deleteArray (i, n) { this.edit('list', [i, n, []]) }
  setMap (k, v) { this.edit('map', [k, v]) }
  transact (f) {
    this.depth++
    f(this)
    this.depth--
    if (this.depth === 0) this.flush()
  }

  /** Returns the whole document state. `applyUpdate` accepts it. */
  getEncodedState () {
    return Buffer.from(JSON.stringify({ text: this.text, list: this.list, map: this.map, diverged: this.diverged, bag: this.bag }))
  }

  /** Records the content that the suite last read. It gives the exact oracle. */
  remember () { bench().last = { text: this.text, list: this.list } }
  getText () { this.remember(); return this.diverged ? [...this.bag].sort().join('|') : this.text }
  getArray () { this.remember(); return this.diverged ? [...this.bag].sort() : this.list }
  getMap () { return this.diverged ? { bag: [...this.bag].sort() } : this.map }
}

const factory = {
  create: handler => new Recorder(handler, bench().nextDoc++),
  load: (handler, bin) => {
    const doc = new Recorder(handler, 'loaded')
    doc.applyUpdate(bin)
    return doc
  },
  getName: () => 'recorder'
}

// B4x100 draws no random numbers, and a plain string is too slow for it. B4 and
// B4x100 come from the upstream file. Both still run in suite order.
await lib.runBenchmarks(factory, name => {
  const run = !name.startsWith('[B4x100')
  if (run) current = name
  return run
})
console.info = info

const sha256 = bytes => createHash('sha256').update(bytes).digest('hex')

/** Writes a trace, and returns the spec fields that name it. */
const writeTrace = (id, trace) => {
  const bytes = Buffer.from(JSON.stringify(trace))
  const relative = `crdt-benchmarks/${id}.trace.json`
  writeFileSync(join(outDir, relative), bytes)
  return { trace: relative, sha256: sha256(bytes) }
}

const workloads = []
const add = (id, description, fields, settings) => workloads.push({ name: id, description, ...fields, ...settings })

for (const [name, b] of benches) {
  const id = name.match(/\[(.*?)\]/)[1].toLowerCase()
  const description = name.replace(/\[.*?\] /, '')
  const base = { kind: 'concurrent', numAgents: b.agents.size, txns: b.txns }
  if (id.startsWith('b1.')) {
    const isList = b.txns[0].listOps !== undefined
    const oracle = isList ? { oracle: { kind: 'exact_list', values: b.last.list } } : { endContent: b.last.text }
    add(id, description, writeTrace(id, { ...base, ...oracle }), { receivers: 1 })
  } else if (id.startsWith('b2.')) {
    add(id, `${description}, as run: the second batch follows the first`,
      writeTrace(id, { ...base, endContent: b.last.text }), { setup_txns: 1 })
    // The same ops with real concurrency. Both batches fork from the setup.
    const forked = b.txns.map((t, i) => (i === 2 ? { ...t, parents: [0] } : t))
    add(`${id}c`, `${description}, truly concurrent: both batches fork from the setup`,
      writeTrace(`${id}c`, { ...base, txns: forked, oracle: { kind: 'tree_invariants' } }), { setup_txns: 1 })
  } else if (id.startsWith('b3.')) {
    // B3.2 writes an object value, and the op model has no object value.
    if (id === 'b3.2') continue
    const kind = id === 'b3.1' || id === 'b3.3' ? 'map_winner' : 'tree_invariants'
    add(id, description, writeTrace(id, { ...base, oracle: { kind } }),
      { timing: 'merge_only', merge_batch: true, check_replicas: 2 })
  }
}

// B4 is the upstream automerge-paper trace. Prove that first.
const load = file => JSON.parse(gunzipSync(readFileSync(join(editingTraces, file))))
const paperFile = 'sequential_traces/automerge-paper.json.gz'
const paper = load(paperFile)
const patches = paper.txns.flatMap(t => t.patches)
const same = patches.length === lib.edits.length &&
  patches.every((p, i) => p[0] === lib.edits[i][0] && p[1] === lib.edits[i][1] && p[2] === (lib.edits[i][2] ?? ''))
if (!same || paper.endContent !== lib.finalText) throw new Error('B4 differs from the upstream automerge-paper trace')

const copyTrace = file => {
  const name = file.split('/').pop()
  copyFileSync(join(editingTraces, file), join(outDir, 'editing-traces', name))
  return { trace: `editing-traces/${name}`, sha256: sha256(readFileSync(join(editingTraces, file))) }
}
add('b4', 'Apply real-world editing dataset', copyTrace(paperFile), {})
add('b4x100', 'Apply real-world editing dataset 100 times', copyTrace(paperFile), { repeat: 100, heavy: true })

const toml = (header, items) => header + items.map(w => {
  const lines = ['[[workload]]']
  for (const [k, v] of Object.entries(w)) lines.push(`${k} = ${JSON.stringify(v)}`)
  return lines.join('\n')
}).join('\n\n') + '\n'

const generated = '# Generated by `just gen-traces`. Do not edit.\n#\n'
writeFileSync(join(repoDir, 'workloads/crdt-benchmarks.toml'), toml(
  generated +
  '# The crdt-benchmarks suite (dmonad/crdt-benchmarks). B3.2 is absent, because it\n' +
  '# writes an object value and the op model has none. A name that ends in c is the\n' +
  '# truly concurrent form of a B2 benchmark. crdt-benchmarks does not run that form.\n\n',
  workloads))

const concurrent = [
  ['et.friendsforever', 'Two users typing with 1 s of latency', 'concurrent_traces/friendsforever.json.gz'],
  ['et.clownschool', 'Users typing with 0.5 s of latency', 'concurrent_traces/clownschool.json.gz']
].map(([name, description, file]) => ({ name, description, ...copyTrace(file) }))
writeFileSync(join(repoDir, 'workloads/editing-traces.toml'), toml(
  generated + '# The concurrent traces of josephg/editing-traces.\n\n', concurrent))

log(`wrote ${workloads.length} crdt-benchmarks workloads and ${concurrent.length} editing-traces workloads`)
