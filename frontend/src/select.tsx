import { React, Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "@a13n/console";

type Option<T extends string> = { value: T; label: string; icon?: React.ReactNode };

export function ChannelSelect<T extends string>({
  id, label, value, options, onChange, disabled, placeholder, required, container,
}: {
  id?: string;
  label: string;
  value: T | null;
  options: Option<T>[];
  onChange: (value: T) => void;
  disabled?: boolean;
  placeholder?: string;
  required?: boolean;
  container?: React.RefObject<HTMLDialogElement | null>;
}) {
  const selected = options.find(option => option.value === value);
  return (
    <Select items={options} value={value} disabled={disabled} required={required}
      onValueChange={(next: T | null) => { if (next !== null) onChange(next); }}>
      <SelectTrigger id={id} aria-label={label} className="a13n-channel-select">
        {selected?.icon}
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectPopup portalProps={container ? { container } : undefined}>
        {options.map(option => <SelectItem key={option.value} value={option.value} label={option.label}>
          <span className="a13n-channel-icon-label">{option.icon}{option.label}</span>
        </SelectItem>)}
      </SelectPopup>
    </Select>
  );
}
