"""Verify period hours-per-call values, labels, sorting and CSV end to end."""
import argparse
import csv
import io
import json
from pathlib import Path

from playwright.sync_api import sync_playwright

FIELD = 'avg_time_between_calls_hours'
LABEL = 'Avg. Time Between Calls (hours)'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8876/')
    parser.add_argument('--output', type=Path, default=Path('runtime-qa/call-spacing'))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='chrome', headless=True)
        page = browser.new_page(viewport={'width':1440,'height':1000}, accept_downloads=True)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto(args.url, wait_until='domcontentloaded', timeout=60000)
        page.locator('#board-personnel tr[data-person-id]').first.wait_for(timeout=60000)
        page.wait_for_function('window.ppfdTypography && ppfdTypography.status().fittedCells>0')
        page.evaluate('document.getElementById("auto").checked=false; document.getElementById("auto").dispatchEvent(new Event("change"))')
        payload = page.evaluate('window.ppfdData')
        checked = 0
        for key in ('today','prior','week','month','year','rolling_year'):
            period = payload[key]
            hours = period['meta']['call_spacing']['period_hours']
            assert hours > 0
            for row in period['rows']:
                calls = row.get('calls') if key in ('today','prior') else row.get('total_calls')
                expected = round(hours/calls,2) if calls else None
                assert row[FIELD] == expected, (key,row['unit'])
                checked += 1
        for key, period in payload['personnel'].items():
            for row in period['rows']:
                hours, calls = row.get('total_hours_worked'), row.get('total_calls')
                expected = round(hours/calls,2) if hours and calls else None
                assert row[FIELD] == expected, (key,row['person_id'])
                checked += 1
        for key in ('today','prior'):
            first = page.locator('#board-'+key+' tbody tr[data-unit]').first
            unit = first.get_attribute('data-unit')
            row = next(row for row in payload[key]['rows'] if row['unit']==unit)
            value = '--' if row[FIELD] is None else f'{row[FIELD]:.2f}'
            assert first.locator('.activity-summary').inner_text().startswith(value+' hrs between calls')
        assert 'calls/hr' not in page.locator('body').inner_text().lower()
        assert page.locator('#board-personnel th[data-sort="'+FIELD+'"]').count()==1
        unit_samples = {}
        for key in ('week','month','year'):
            first = page.locator('#board-'+key+' tbody tr[data-unit]').first
            unit = first.get_attribute('data-unit')
            row = next(row for row in payload[key]['rows'] if row['unit']==unit)
            expected = '--' if row[FIELD] is None else f'{row[FIELD]:.2f}'
            assert first.locator('td').nth(2).inner_text()==expected
            unit_samples[key] = {'unit':unit,'hours':row[FIELD]}
        page.screenshot(path=str(args.output/'desktop.png'))
        for period in payload['personnel']:
            page.select_option('#personnelPeriod',period)
            first = page.locator('#board-personnel tbody tr[data-person-id]').first
            pid = first.get_attribute('data-person-id')
            row = next(row for row in payload['personnel'][period]['rows'] if str(row['person_id'])==pid)
            expected = '--' if row[FIELD] is None else f'{row[FIELD]:.2f}'
            assert first.locator('td').nth(3).inner_text()==expected
        page.select_option('#personnelPeriod','backfill')
        page.locator('#board-personnel th[data-sort="'+FIELD+'"]').click()
        values = page.locator('#board-personnel tbody tr[data-person-id] td:nth-child(4)').all_inner_texts()
        numbers = [float(value) for value in values if value!='--']
        assert numbers == sorted(numbers), 'First interval sort must put the smallest interval first'
        if '--' in values:
            assert values.index('--') == len(numbers), 'Unknown values should sort last'
        exports = {}
        for panel in ('today','week','personnel'):
            try:
                with page.expect_download(timeout=10000) as download:
                    page.locator('.csv-btn[data-panel="'+panel+'"]').click()
            except Exception:
                print(json.dumps({'failed_export':panel,'browser_errors':errors}),flush=True)
                raise
            path = args.output/(panel+'.csv')
            download.value.save_as(path)
            rows = list(csv.DictReader(io.StringIO(path.read_text(encoding='utf-8-sig'))))
            assert rows and LABEL in rows[0], (panel,'New column missing from CSV')
            assert all('Calls/Hr' not in key for key in rows[0])
            exports[panel] = {'rows':len(rows),'sample':rows[0][LABEL]}
        page.set_viewport_size({'width':390,'height':844})
        page.locator('#panel-today').scroll_into_view_if_needed()
        page.screenshot(path=str(args.output/'mobile.png'))
        assert not errors, errors
        report = {'url':args.url,'validated_rows':checked,'units':unit_samples,'exports':exports,
                  'definition':'Period/working hours per call, not clear-to-next-call downtime',
                  'sorting_periods_labels_csv_mobile':'passed','browser_errors':errors}
        (args.output/'acceptance.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
        print(json.dumps(report,indent=2))
        browser.close()


if __name__=='__main__':
    main()
