USERNAME_INPUTS = [
    "input[placeholder='Usuario']",
    "input[name='username']",
    "input[name='user']",
    "input[type='email']",
    "input[type='text']",
]

PASSWORD_INPUTS = [
    "input[placeholder='Contraseña']",
    "input[name='password']",
    "input[type='password']",
]

SUBMIT_BUTTONS = [
    "button:has-text('Iniciar')",
    "button[type='submit']",
    "input[type='submit']",
    "button:has-text('Ingresar')",
    "button:has-text('Entrar')",
    "button:has-text('Login')",
]

SUCCESS_LANDMARKS = [
    "h3:has-text('Sucursales')",
    "text=Sucursales",
    "nav",
    "text=Inventario",
    "[data-testid='sidebar']",
]

ERROR_BANNERS = [
    ".alert-danger",
    ".error",
    "[role='alert']",
]
