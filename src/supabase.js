import { createClient } from '@supabase/supabase-js'

export const supabase = createClient(
  'https://ykvvbbyatitvntkhdiae.supabase.co',
  import.meta.env.VITE_SUPABASE_ANON_KEY || ''
)
