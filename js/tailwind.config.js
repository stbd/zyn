/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["../py/zyn/client/zyn-web-templates/*html", "src/*js"],
  theme: {
    extend: {
      colors: {
        'zyn_green': '#bbf7d0'
      },
    }
  },
  plugins: [],
}
