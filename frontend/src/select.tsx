import { React, Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "@a13n/console";

type Option<T extends string> = { value: T; label: string };

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
  return (
    <Select items={options} value={value} disabled={disabled} required={required}
      onValueChange={(next: T | null) => { if (next !== null) onChange(next); }}>
      <SelectTrigger id={id} aria-label={label} className="a13n-channel-select">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectPopup portalProps={container ? { container } : undefined}>
        {options.map(option => <SelectItem key={option.value} value={option.value}>{option.label}</SelectItem>)}
      </SelectPopup>
    </Select>
  );
}
