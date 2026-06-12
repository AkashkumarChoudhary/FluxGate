import { describe, it, expect } from 'vitest';
import { renderBody } from './template';

describe('renderBody', () => {
  it('substitutes {{payload.x}} placeholders from the event payload', () => {
    const out = renderBody({ msg: 'hello {{payload.name}}' }, { name: 'akash' });
    expect(out).toEqual({ msg: 'hello akash' });
  });
  it('passes the raw payload through when no template is configured', () => {
    expect(renderBody(undefined, { a: 1 })).toEqual({ a: 1 });
  });
  it('leaves unknown placeholders empty', () => {
    expect(renderBody({ msg: '{{payload.missing}}' }, {})).toEqual({ msg: '' });
  });
});
