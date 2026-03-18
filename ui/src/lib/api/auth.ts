import { apiGet, apiPost } from './client'
import type { Session } from '../types/api'

interface LoginParams {
  username: string
  password: string
}

interface LoginApiResponse {
  success: boolean
  user: { id: string; username: string; role: string }
}

/** Log in to Patolake */
export async function login(params: LoginParams): Promise<{ session: Session }> {
  const res = await apiPost<LoginApiResponse>('/api/auth/login', params)
  return {
    session: {
      authenticated: true,
      user: res.user,
    },
  }
}

/** Log out and destroy the session */
export function logout(): Promise<void> {
  return apiPost('/api/auth/logout')
}

/** Check if a valid session exists */
export async function checkSession(): Promise<Session | null> {
  try {
    const res = await apiGet<Session>('/api/auth/session')
    if (!res.authenticated) return null
    return res
  } catch {
    return null
  }
}
