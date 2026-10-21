"""Compare fitted type against the exact original layout in the same browser."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


SNAPSHOT = """() => {
  const selector='.wrap,.toolbar-shell,.toolbar-shortcuts,.toolbar-toggle,.canvas,.panel,.panel .hdr,.panel .hdr>span,.panel .body,.panel table,.panel tr,.panel th,.panel td,.activity-timeline,.activity-scale,.health-grid,.health-grid .k,.health-grid .v,.personnel-controls,.footer,#rosterPop,.roster-head,.roster-entry,.roster-entry .time,.roster-entry .name,.roster-entry .meta,.personnel-calls-dialog,.personnel-calls-dialog header,.personnel-calls-dialog footer';
  return [...document.querySelectorAll(selector)].filter(e=>e.getClientRects().length).map((e,i)=>{
    const r=e.getBoundingClientRect(),s=getComputedStyle(e);
    return {key:e.id||e.tagName+'-'+i,x:r.x+scrollX,y:r.y+scrollY,w:r.width,h:r.height,padding:s.padding,gap:s.gap};
  });
}"""


def layout_differences(before, after):
    assert len(before) == len(after)
    return [{'before':old, 'after':new} for old,new in zip(before, after)
            if any(abs(old[key]-new[key]) > 0.6 for key in ('x','y','w','h'))
            or old['padding'] != new['padding'] or old['gap'] != new['gap']]


def compare_current_layout(page):
    page.evaluate('ppfdTypography.disable()')
    page.evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
    before = page.evaluate(SNAPSHOT)
    extent = page.evaluate('({w:document.documentElement.scrollWidth,h:document.documentElement.scrollHeight})')
    page.evaluate('ppfdTypography.enable()')
    changes = layout_differences(before, page.evaluate(SNAPSHOT))
    assert page.evaluate('({w:document.documentElement.scrollWidth,h:document.documentElement.scrollHeight})') == extent
    assert not changes, changes[:3]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8876/')
    parser.add_argument('--output', type=Path, default=Path('runtime-qa/typography'))
    parser.add_argument('--widths', type=int, nargs='+', default=[1920,1440,768,390])
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='chrome', headless=True)
        for width in args.widths:
            page = browser.new_page(viewport={'width':width, 'height':1000})
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto(args.url, wait_until='domcontentloaded', timeout=60000)
            page.locator('#board-personnel tr[data-person-id]').first.wait_for(timeout=60000)
            page.wait_for_function('window.ppfdTypography && ppfdTypography.status().fittedCells > 0')
            page.evaluate('ppfdTypography.disable(); document.getElementById("auto").checked=false; document.getElementById("auto").dispatchEvent(new Event("change"))')
            page.evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
            before = page.evaluate(SNAPSHOT)
            page.screenshot(path=str(args.output / f'{width}-before.png'), full_page=False)
            page.evaluate('ppfdTypography.enable()')
            after = page.evaluate(SNAPSHOT)
            changes = layout_differences(before, after)
            metrics = page.evaluate("""() => ({...ppfdTypography.status(),
              daily_unit: getComputedStyle(document.querySelector('#board-today tbody td .ppfd-fit-content')).fontSize,
              daily_calls: getComputedStyle(document.querySelector('#board-today .calls-badge')||document.querySelector('#board-today tbody td:nth-child(2) .ppfd-fit-content')).fontSize,
              daily_row:document.querySelector('#board-today tbody tr').getBoundingClientRect().height,
              badHeaderText:[...document.querySelectorAll('.panel .hdr>span:not(.dot)')].filter(e=>{
                const next=e.nextElementSibling;if(!next||!e.textContent.trim())return false;
                const range=document.createRange();range.selectNodeContents(e);
                return range.getBoundingClientRect().right>next.getBoundingClientRect().left-2;
              }).map(e=>e.textContent),
              badCells:[...document.querySelectorAll('.ppfd-fit-content')].filter(e=>e.scrollWidth>e.clientWidth+1).length})""")
            page.screenshot(path=str(args.output / f'{width}-after.png'), full_page=False)
            if width == 1440:
                page.locator('#board-week').scroll_into_view_if_needed()
                page.screenshot(path=str(args.output / 'summary-tables.png'))
                page.locator('#board-personnel').scroll_into_view_if_needed()
                page.screenshot(path=str(args.output / 'personnel-table.png'))
                for selector, value in (('#personnelPeriod','week'), ('#yearMode','rolling_year')):
                    revision = page.evaluate('ppfdTypography.status().revision')
                    page.select_option(selector, value)
                    page.wait_for_function('n=>ppfdTypography.status().revision>n', arg=revision)
                    compare_current_layout(page)
                revision = page.evaluate('ppfdTypography.status().revision')
                page.locator('#board-personnel th[data-sort="name"]').click()
                page.wait_for_function('n=>ppfdTypography.status().revision>n', arg=revision)
                compare_current_layout(page)
                revision = page.evaluate('ppfdTypography.status().revision')
                page.locator('#personnelFilter').fill('Wyatt')
                page.wait_for_function('n=>ppfdTypography.status().revision>n', arg=revision)
                compare_current_layout(page)
                page.locator('#personnelFilter').fill('')
                available_unit = page.locator('#board-today tr[data-unit]').first
                if available_unit.count():
                    available_unit.click()
                    page.locator('#rosterPop').wait_for(state='visible')
                    compare_current_layout(page)
                    page.locator('#rosterClose').click()
                page.set_viewport_size({'width':390,'height':844})
                page.evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
                compare_current_layout(page)
                metrics['sorting_filtering_periods_popup_resize'] = 'passed'
            result = {'width':width, 'layout_changes':changes[:12], 'changed_count':len(changes), 'metrics':metrics, 'browser_errors':errors}
            results.append(result)
            print(json.dumps(result, indent=2), flush=True)
            page.close()
        browser.close()
    (args.output / 'acceptance.json').write_text(json.dumps(results, indent=2)+'\n', encoding='utf-8')
    assert not any(row['changed_count'] or row['browser_errors'] or row['metrics']['badCells'] or row['metrics']['badHeaderText'] or row['metrics'].get('error') for row in results), 'Type fitting changed layout or overflowed; see acceptance.json'


if __name__ == '__main__':
    main()
