"use client"

import * as React from "react"
import { X } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"

interface TagInputProps {
  value: string[]
  onChange: (tags: string[]) => void
  suggestions?: string[]
  placeholder?: string
  disabled?: boolean
  id?: string
}

export function TagInput({
  value,
  onChange,
  suggestions = [],
  placeholder = "Add tag...",
  disabled,
  id,
}: TagInputProps) {
  const [draft, setDraft] = React.useState("")

  const addTag = (raw: string) => {
    const tag = raw.trim()
    if (!tag || value.includes(tag)) return
    onChange([...value, tag])
  }

  const removeTag = (tag: string) => onChange(value.filter(valueTag => valueTag !== tag))

  const commitDraft = () => {
    if (!draft.trim()) return
    addTag(draft)
    setDraft("")
  }

  const available = Array.from(new Set(suggestions.map(tag => tag.trim())))
    .filter(tag => tag && !value.includes(tag))

  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <div className="flex min-h-7 flex-wrap gap-1.5">
        {value.length === 0 && (
          <span className="text-xs italic text-muted-foreground">No tags</span>
        )}
        {value.map(tag => (
          <Badge key={tag} variant="secondary" className="max-w-full gap-1 pr-1">
            <span className="max-w-60 truncate" title={tag}>{tag}</span>
            <button
              type="button"
              className="rounded p-0.5 hover:bg-muted-foreground/20"
              onClick={() => removeTag(tag)}
              disabled={disabled}
              title={`Remove ${tag}`}
              aria-label={`Remove ${tag}`}
            >
              <X className="h-3 w-3" />
            </button>
          </Badge>
        ))}
      </div>
      <Input
        id={id}
        value={draft}
        placeholder={placeholder}
        disabled={disabled}
        onChange={event => setDraft(event.target.value)}
        onKeyDown={event => {
          if (event.key === "Enter" || event.key === ",") {
            event.preventDefault()
            commitDraft()
          } else if (event.key === "Backspace" && !draft && value.length > 0) {
            removeTag(value[value.length - 1])
          }
        }}
        onBlur={commitDraft}
      />
      {available.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {available.slice(0, 12).map(tag => (
            <Button
              key={tag}
              type="button"
              size="sm"
              variant="outline"
              disabled={disabled}
              className="h-6 max-w-48 px-2 text-xs"
              onClick={() => addTag(tag)}
              title={tag}
            >
              <span className="truncate">{tag}</span>
            </Button>
          ))}
        </div>
      )}
    </div>
  )
}
