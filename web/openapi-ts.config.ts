import { defineConfig } from '@hey-api/openapi-ts'

export default defineConfig({
  input: '../internal/web/api/api.yml',
  output: 'src/generated',
  plugins: ['@hey-api/typescript'],
})
