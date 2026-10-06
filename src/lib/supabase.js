import { createClient } from '@supabase/supabase-js'

const anonKey = import.meta.env.VITE_SUPABASE_ANON_KEY

// 未設定 key 時仍建立 client（避免 throw），由 supabaseConfigured 讓 UI 顯示提示
export const supabaseConfigured = Boolean(anonKey)

export const supabase = createClient(
  'https://ykvvbbyatitvntkhdiae.supabase.co',
  anonKey || 'not-configured'
)
