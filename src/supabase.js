import { createClient } from '@supabase/supabase-js'

// Fallback prevents createClient from throwing when env var is not yet set
export const supabase = createClient(
  'https://ykvvbbyatitvntkhdiae.supabase.co',
  import.meta.env.VITE_SUPABASE_ANON_KEY || 'not-configured'
)
