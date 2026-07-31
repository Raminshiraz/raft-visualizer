/* Mounts the app. Must be concatenated last.
 *
 * Part of the single classic script build.mjs emits — every file in src/
 * shares one top-level scope and is concatenated in the order listed there.
 * No imports, no exports: index.html loads app.js as a plain <script>, so
 * the page runs from file:// with no bundler and no module server.
 */
try {
  ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
} catch(err){
  window.__raftPanic('Render failed',(err&&err.stack)||String(err));
}
