import * as esbuild from 'esbuild'

await esbuild.build({
  entryPoints: ['src/main.js'],
  bundle: true,
  outfile: 'zyn.js',
  globalName: 'zyn',
  platform: 'browser',
  minify: true,
})