type Theme = 'dark' | 'light'

const initial = (localStorage.getItem('patolake-theme') as Theme) || 'dark'
let theme = $state<Theme>(initial)

applyTheme(initial)

export function getTheme(): Theme {
  return theme
}

export function toggleTheme(): void {
  theme = theme === 'dark' ? 'light' : 'dark'
  localStorage.setItem('patolake-theme', theme)
  applyTheme(theme)
}

function applyTheme(t: Theme): void {
  if (t === 'dark') {
    document.documentElement.classList.add('dark')
  } else {
    document.documentElement.classList.remove('dark')
  }
}
