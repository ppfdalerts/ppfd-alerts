"""Browser acceptance tests for the existing public/local dashboard."""
import argparse
import csv
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8876/")
    parser.add_argument("--output", type=Path, default=Path("runtime-qa"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel="chrome", headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 1000}, accept_downloads=True)
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
        page.locator('#board-personnel tr[data-person-id]').first.wait_for(timeout=60000)
        assert page.locator('#personnelPeriod').input_value() == 'backfill'
        assert page.locator('#rosterCalendarLink').count() == 1
        payload = page.evaluate('window.ppfdData')
        row = page.locator('#board-personnel tr[data-person-id]').first
        pid = row.get_attribute('data-person-id')
        ui_count = int(row.locator('td').nth(2).inner_text())
        row.click()
        dialog = page.locator('#personnelCallsDialog')
        dialog.wait_for(state='visible')
        page.wait_for_function("document.getElementById('personnelCallsCount').textContent.includes(' calls')")
        history = page.evaluate("fetch('personnel_calls.json?qa='+Date.now(),{cache:'no-store'}).then(r=>r.json())")
        calls = history['people'][pid]['calls']
        assert ui_count == len(calls)
        assert len({call['incident_id'] for call in calls}) == len(calls)
        assert page.locator('#personnelCallsRows tr').count() == min(50, len(calls))
        page.screenshot(path=str(args.output / 'personnel-calls-desktop.png'))
        if calls:
            page.locator('#personnelCallsSearch').fill(calls[0]['incident_id'])
            assert page.locator('#personnelCallsRows tr').count() == 1
            with page.expect_download() as download:
                page.locator('#personnelCallsCsv').click()
            path = args.output / 'verified-call.csv'
            download.value.save_as(path)
            with path.open(encoding='utf-8-sig', newline='') as handle:
                csv_rows = list(csv.DictReader(handle))
            assert len(csv_rows) == 1 and csv_rows[0]['Incident'] == calls[0]['incident_id']
        page.set_viewport_size({"width": 390, "height": 844})
        box = dialog.bounding_box()
        assert box and box['width'] <= 390 and box['x'] >= 0
        page.screenshot(path=str(args.output / 'personnel-calls-mobile.png'))
        page.locator('#personnelCallsClose').click()
        page.set_viewport_size({"width": 1440, "height": 1000})
        roster = page.evaluate("fetch('roster_units.json?qa='+Date.now(),{cache:'no-store'}).then(r=>r.json())")
        assert roster['today']['date'] == history['through']
        units_tested = {}
        for period in ('today', 'prior'):
            selected = roster[period]
            source = page.evaluate("day => fetch('staffing/roster_units_'+day+'.json?qa='+Date.now(),{cache:'no-store'}).then(r=>r.json())", selected['date'])
            expected = {}
            for source_unit in source['units']:
                code = source_unit['unit_code'].replace('DC', 'D').replace('TR', 'T')
                expected.setdefault(code, []).extend(source_unit.get('entries', []))
            assert {unit['unit_code']: unit['entries'] for unit in selected['units']} == expected, period + ' mixes roster dates'
            available_units = [unit for unit in selected['units'] if unit.get('entries') and page.locator('#board-'+period+' tr[data-unit="'+unit['unit_code']+'"]').count()]
            assert available_units
            unit = available_units[0]
            page.locator('#board-'+period+' tr[data-unit="'+unit['unit_code']+'"]').click()
            page.locator('#rosterPop').wait_for(state='visible')
            assert selected['date'] in page.locator('#rosterMeta').inner_text()
            for entry in unit['entries']:
                assert entry['name'] in page.locator('#rosterBody').inner_text()
            assert page.locator('#rosterBody .roster-entry').count() == len(unit['entries'])
            page.screenshot(path=str(args.output / ('unit-staffing-'+period+'.png')))
            units_tested[period] = {'unit': unit['unit_code'], 'date': selected['date'], 'crew_count': len(unit['entries'])}
            page.locator('#rosterClose').click()
        assert not errors, errors
        report = {"url": args.url, "personnel_count": len(history['people']),
                  "personnel_calls": sum(len(person['calls']) for person in history['people'].values()),
                  "sample_person_calls": ui_count, "units_tested": units_tested,
                  "roster_date": roster['today']['date'], "browser_errors": errors,
                  "desktop_mobile_csv_unit_click": "passed", "coverage": payload['personnel_history']}
        (args.output / 'browser-acceptance.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
        print(json.dumps(report, indent=2))
        browser.close()


if __name__ == '__main__':
    main()
