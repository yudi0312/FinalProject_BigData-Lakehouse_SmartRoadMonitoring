export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        civic: {
          50: "#eff8ff",
          100: "#dbeefe",
          500: "#1784d8",
          600: "#0e67b5",
          700: "#0d528f",
          900: "#123452"
        }
      },
      boxShadow: {
        panel: "0 18px 50px rgba(15, 60, 105, 0.12)"
      }
    },
  },
  plugins: [],
};
