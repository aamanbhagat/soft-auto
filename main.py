import asyncio
import os
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, TextColumn
from rich.table import Table
from tenacity import retry, stop_after_delay, wait_fixed
from fingerprints import random_profile
import contextlib
import subprocess
import shutil
import stat

console = Console()

MAX_WAIT = 8000  # ms, per step
POLL_MS = 100  # element polling interval
MAX_FIND_SEC = 60  # per element
HEADLESS = True
INSTANCES = 3  # default; will be overridden by user prompt
VERBOSE = False  # suppress all prints except the live dashboard


@dataclass
class StepResult:
    step: str
    status: str
    detail: str = ""


@dataclass
class InstanceState:
    id: int
    runs: int = 0
    successes: int = 0
    failures: int = 0
    current_step: str = "idle"
    last_url: str = ""
    last_detail: str = ""
    status: str = "idle"
    started_at: float = field(default_factory=time.time)
    last_duration: float = 0.0


def nice_table(title: str, rows: list[tuple[str, str]]):
    table = Table(title=title, show_header=True, header_style="bold magenta")
    table.add_column("Key", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")
    for k, v in rows:
        table.add_row(k, v)
    console.print(table)


async def hard_click(page, selector: str, description: str, many_selectors: list[str] | None = None, double: bool = False) -> None:
    """Hard, real-like click with many strategies and live logs.

    - Poll every 100ms up to 60s using multiple selectors
    - Scroll into view, focus, mouse move, down/up, force click
    - JS click + dispatch pointer events as fallback
    - Optional double click for stubborn buttons
    """
    selectors = [selector] + (many_selectors or [])
    deadline = time.time() + MAX_FIND_SEC
    last_err: Optional[Exception] = None
    last_log = 0.0

    while time.time() < deadline:
        for sel in selectors:
            now = time.time()
            if VERBOSE and now - last_log > 1.0:
                console.print(f"[yellow]Searching[/yellow] for [cyan]{description}[/cyan] using selector: [white]{sel}[/white] t={now:.0f}")
                last_log = now
            try:
                el = await page.wait_for_selector(sel, timeout=POLL_MS, state="visible")
                if not el:
                    continue
                try:
                    await el.scroll_into_view_if_needed()
                except Exception:
                    pass
                # Try element.click with different forces
                for force in (True, False):
                    try:
                        await el.click(force=force, timeout=MAX_WAIT)
                        if VERBOSE:
                            console.print(f"[green]Clicked[/green] {description} via element.click(force={force})")
                        return
                    except Exception as e:
                        last_err = e
                # Try double click if requested
                if double:
                    try:
                        await el.dblclick(timeout=MAX_WAIT)
                        if VERBOSE:
                            console.print(f"[green]Double-clicked[/green] {description} via element.dblclick()")
                        return
                    except Exception as e:
                        last_err = e
                # Try mouse interaction at element center with human-like behavior
                try:
                    box = await el.bounding_box()
                except Exception:
                    box = None
                if box:
                    try:
                        # Add small random offset for more human-like clicking
                        x = box["x"] + box["width"] * (0.3 + random.random() * 0.4)
                        y = box["y"] + box["height"] * (0.3 + random.random() * 0.4)
                        
                        # Human-like mouse movement and click timing
                        await page.mouse.move(x, y)
                        await asyncio.sleep(random.uniform(0.05, 0.15))  # Brief pause
                        await page.mouse.down()
                        await asyncio.sleep(random.uniform(0.05, 0.12))  # Hold time
                        await page.mouse.up()
                        
                        if VERBOSE:
                            console.print(f"[green]Human-like mouse click[/green] on {description} at ({x:.0f},{y:.0f})")
                        return
                    except Exception as e:
                        last_err = e
                # Try page.click selector directly
                try:
                    await page.click(sel, timeout=MAX_WAIT, force=True)
                    if VERBOSE:
                        console.print(f"[green]Clicked[/green] {description} via page.click(force=True)")
                    return
                except Exception as e:
                    last_err = e
                # Try JS click and dispatch events
                try:
                    await page.evaluate(
                        "el => { el.dispatchEvent(new MouseEvent('mousedown',{bubbles:true})); el.dispatchEvent(new MouseEvent('mouseup',{bubbles:true})); el.click(); }",
                        el,
                    )
                    if VERBOSE:
                        console.print(f"[green]Clicked[/green] {description} via JS dispatch")
                    return
                except Exception as e:
                    last_err = e
            except PlaywrightTimeout as e:
                last_err = e
                await asyncio.sleep(POLL_MS / 1000)
                continue
            except Exception as e:
                last_err = e
                await asyncio.sleep(POLL_MS / 1000)
                continue
        await asyncio.sleep(POLL_MS / 1000)
    raise last_err or RuntimeError(f"Element not found/clickable: {description}")


async def wait_for_load(page, max_ms: int = MAX_WAIT):
    # Wait for network mostly idle but cap by max_ms
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=max_ms)
    except Exception:
        pass
    try:
        await page.wait_for_load_state("networkidle", timeout=500)
    except Exception:
        pass

async def wait_for_any_visible(page, selectors: list[str], max_seconds: int = MAX_FIND_SEC) -> Optional[str]:
    """Poll multiple selectors every POLL_MS until one becomes visible; return that selector or None."""
    deadline = time.time() + max_seconds
    while time.time() < deadline:
        for sel in selectors:
            try:
                el = await page.wait_for_selector(sel, timeout=POLL_MS, state="visible")
                if el:
                    return sel
            except PlaywrightTimeout:
                pass
            except Exception:
                pass
        await asyncio.sleep(POLL_MS / 1000)
    return None


def _on_rm_error(func, path, exc_info):
    # On Windows, remove read-only flag and retry
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception:
        pass


def wipe_dir(path: Path, attempts: int = 3, delay: float = 0.3):
    for _ in range(attempts):
        if not path.exists():
            return
        try:
            shutil.rmtree(path, onerror=_on_rm_error)
            return
        except Exception:
            time.sleep(delay)
    # Fallback: best-effort empty contents
    if path.exists():
        for p in sorted(path.rglob('*'), reverse=True):
            try:
                if p.is_file() or p.is_symlink():
                    os.chmod(p, stat.S_IWRITE)
                    p.unlink(missing_ok=True)
                elif p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
            except Exception:
                pass

def read_random_url(file_path: str) -> str:
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    if not lines:
        raise ValueError("No URLs found in the file")
    return random.choice(lines)


async def run_once(url: str, instance_id: int, _unused_user_data_dir: Path, state: InstanceState) -> list[StepResult]:
    results: list[StepResult] = []
    start_time = time.time()
    async with async_playwright() as p:
        state.current_step = "launching"
        browser = None
        context = None
        page = None
        try:
            try:
                browser = await p.chromium.launch(
                    headless=HEADLESS, 
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-extensions",
                        "--disable-plugins",
                        "--disable-images",
                        "--disable-javascript-harmony-shipping",
                        "--disable-background-timer-throttling",
                        "--disable-renderer-backgrounding",
                        "--disable-backgrounding-occluded-windows",
                        "--disable-ipc-flooding-protection",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--no-pings",
                        "--password-store=basic",
                        "--use-mock-keychain",
                        "--disable-component-extensions-with-background-pages",
                        "--disable-default-apps",
                        "--mute-audio",
                        "--no-zygote",
                        "--disable-background-networking",
                        "--disable-web-security",
                        "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                        "--hide-scrollbars",
                        "--disable-gpu"
                    ]
                )  # type: ignore
            except Exception:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=False)
                browser = await p.chromium.launch(
                    headless=HEADLESS, 
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-extensions",
                        "--disable-plugins",
                        "--disable-images",
                        "--disable-javascript-harmony-shipping",
                        "--disable-background-timer-throttling",
                        "--disable-renderer-backgrounding",
                        "--disable-backgrounding-occluded-windows",
                        "--disable-ipc-flooding-protection",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--no-pings",
                        "--password-store=basic",
                        "--use-mock-keychain",
                        "--disable-component-extensions-with-background-pages",
                        "--disable-default-apps",
                        "--mute-audio",
                        "--no-zygote",
                        "--disable-background-networking",
                        "--disable-web-security",
                        "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                        "--hide-scrollbars",
                        "--disable-gpu"
                    ]
                )  # type: ignore
            
            fp = random_profile()
            context = await browser.new_context(
                user_agent=fp["user_agent"],
                viewport=fp["viewport"],
                device_scale_factor=fp["device_scale_factor"],
                is_mobile=fp["is_mobile"],
                has_touch=fp["has_touch"],
                locale=fp["locale"],
                timezone_id=fp["timezone_id"],
                color_scheme=fp["color_scheme"],
                reduced_motion=fp["reduced_motion"],
                extra_http_headers=fp["headers"],
                java_script_enabled=True,
                ignore_https_errors=True,
                permissions=['geolocation', 'notifications'],
                geolocation={
                    "latitude": random.uniform(25.0, 49.0), 
                    "longitude": random.uniform(-125.0, -66.0)
                }
            )
            
            # Inject comprehensive stealth script on every page
            await context.add_init_script(fp["stealth_script"])
            
            # Add behavioral simulation for human-like interaction
            await context.add_init_script(f"""
                // Realistic mouse movement simulation
                let mouseX = {random.randint(100, 800)};
                let mouseY = {random.randint(100, 600)};
                
                function simulateHumanMouse() {{
                    const deltaX = (Math.random() - 0.5) * 20;
                    const deltaY = (Math.random() - 0.5) * 20;
                    mouseX = Math.max(0, Math.min(window.innerWidth, mouseX + deltaX));
                    mouseY = Math.max(0, Math.min(window.innerHeight, mouseY + deltaY));
                    
                    const moveEvent = new MouseEvent('mousemove', {{
                        clientX: mouseX,
                        clientY: mouseY,
                        bubbles: true
                    }});
                    document.dispatchEvent(moveEvent);
                }}
                
                // Random mouse movements every 1-3 seconds
                setInterval(simulateHumanMouse, {random.randint(1000, 3000)});
                
                // Simulate scroll behavior
                let scrollPos = 0;
                function simulateScrollPattern() {{
                    if (Math.random() < 0.3) {{ // 30% chance to scroll
                        const direction = Math.random() < 0.7 ? 1 : -1; // 70% down, 30% up
                        const scrollAmount = Math.random() * 100 + 50;
                        scrollPos += direction * scrollAmount;
                        scrollPos = Math.max(0, Math.min(document.body.scrollHeight, scrollPos));
                        window.scrollTo(0, scrollPos);
                    }}
                }}
                setInterval(simulateScrollPattern, {random.randint(2000, 5000)});
                
                console.log('🤖 Human behavior simulation active');
            """)
            
            page = await context.new_page()
            
            # Block images, fonts, and media to save bandwidth
            await page.route("**/*", lambda route: (
                route.abort() if route.request.resource_type in [
                    "image", "media", "font", "stylesheet", "websocket", "manifest"
                ] else route.continue_()
            ))
            
            # Additional page-level stealth enhancements
            await page.evaluate("""
                // Override common automation detection points
                delete window.navigator.webdriver;
                
                // Safely handle chrome.runtime
                if (window.chrome && window.chrome.runtime) {
                    delete window.chrome.runtime.onConnect;
                }
                
                // Simulate realistic timing
                const originalSetTimeout = window.setTimeout;
                window.setTimeout = function(fn, delay, ...args) {
                    const humanDelay = delay + Math.random() * 50 - 25; // ±25ms variation
                    return originalSetTimeout(fn, Math.max(0, humanDelay), ...args);
                };
            """)

            state.current_step = "open url"
            await page.goto(url, wait_until="domcontentloaded", timeout=MAX_WAIT)
            await wait_for_load(page)
            results.append(StepResult("Open URL", "OK", url))

            # Step 2: click human verification on mtc2
            state.current_step = "step 2: mtc2 verify"
            await hard_click(
                page,
                "a#wpsafelinkhuman[href*='mtc2.smsopdappointment.com']",
                "human verification (mtc2)",
                many_selectors=[
                    "a#wpsafelinkhuman",
                    "a[onclick*='wpsafehuman']",
                    "img[alt='human verification']",
                    "a:has(img[alt='human verification'])",
                    "xpath=//a[@id='wpsafelinkhuman']",
                ],
            )
            await wait_for_load(page)
            results.append(StepResult("Step 2", "OK"))

            # Step 3: click generate (mtc2)
            state.current_step = "step 3: mtc2 generate"
            await hard_click(
                page,
                "img[alt*='GENERATE LINK']",
                "generate link (mtc2)",
                many_selectors=[
                    "img[alt*='Generate']",
                    "img[src*='generate']",
                    "img[alt*='CLICK 2X FOR GENERATE LINK']",
                    "xpath=//img[contains(@alt,'GENERATE') or contains(@src,'generate')]",
                ],
                double=True,
            )
            await wait_for_load(page)
            results.append(StepResult("Step 3", "OK"))

            # Step 4: click download target (mtc2)
            state.current_step = "step 4: mtc2 target"
            await hard_click(
                page,
                "a:has(img#image3[alt*='DOWNLOAD LINK'])",
                "download link (mtc2)",
                many_selectors=[
                    "img#image3[alt*='DOWNLOAD LINK']",
                    "img[src*='target']",
                    "a[rel='nofollow'] img#image3",
                    "xpath=//img[@id='image3' and contains(@alt,'DOWNLOAD')]",
                ],
            )
            await wait_for_load(page)
            results.append(StepResult("Step 4", "OK"))

            # Step 5: human verify (mtc1) with parallel presence watch for Step 8 'Get Link'
            state.current_step = "step 5 watch: mtc1 verify vs get-link"

            get_link_selectors = [
                "a.get-link",
                "a.btn.get-link",
                "a.btn-success.get-link",
                "a:has-text('Get Link')",
                "div:has(a.get-link) a",
                "a[href*='mosco.co.in'][class*='get-link']",
                "xpath=//a[contains(@class,'get-link') or normalize-space(text())='Get Link']",
            ]
            step5_selectors = [
                "a#wpsafelinkhuman[href*='mtc1.heygirlish.com']",
                "a#wpsafelinkhuman",
                "a[onclick*='wpsafehuman']",
                "img[alt='human verification']",
                "xpath=//a[@id='wpsafelinkhuman']",
            ]

            # Race: presence only; whichever appears first determines the path
            get_task = asyncio.create_task(wait_for_any_visible(page, get_link_selectors))
            step5_task = asyncio.create_task(wait_for_any_visible(page, step5_selectors))
            done, pending = await asyncio.wait({get_task, step5_task}, return_when=asyncio.FIRST_COMPLETED)

            # Identify winner and cancel the other to avoid future aheads
            winner = next(iter(done))
            for p in pending:
                p.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await p

            if winner is get_task and get_task.result():
                state.current_step = "step 8: pre-wait load + 5s"
                await wait_for_load(page)
                await asyncio.sleep(5)
                await hard_click(
                    page,
                    get_link_selectors[0],
                    "Get Link final",
                    many_selectors=get_link_selectors[1:],
                )
                await asyncio.sleep(1)
                results.append(StepResult("Step 5-8 fast path", "OK", "Get Link appeared first; waited 5s before click"))
                state.current_step = "closing"
                return results
            elif winner is step5_task and step5_task.result():
                state.current_step = "step 5: mtc1 verify click"
                await hard_click(
                    page,
                    step5_selectors[0],
                    "human verification (mtc1)",
                    many_selectors=step5_selectors[1:],
                )
                await wait_for_load(page)
                results.append(StepResult("Step 5", "OK"))
            else:
                try:
                    g = get_task.result()
                except Exception as e:
                    g = e
                try:
                    s5 = step5_task.result()
                except Exception as e:
                    s5 = e
                raise RuntimeError(f"Neither step 5 nor step 8 clickable at this time. get_link={g}, step5={s5}")

            # Step 6: click generate (mtc1)
            state.current_step = "step 6: mtc1 generate"
            await hard_click(
                page,
                "img[alt*='GENERATE LINK']",
                "generate link (mtc1)",
                many_selectors=[
                    "img[alt*='Generate']",
                    "img[src*='generate']",
                    "img[alt*='CLICK 2X FOR GENERATE LINK']",
                    "xpath=//img[contains(@alt,'GENERATE') or contains(@src,'generate')]",
                ],
                double=True,
            )
            await wait_for_load(page)
            results.append(StepResult("Step 6", "OK"))

            # Step 7: click download target (mtc1)
            state.current_step = "step 7: mtc1 target"
            await hard_click(
                page,
                "a:has(img#image3[alt*='DOWNLOAD LINK'])",
                "download link (mtc1)",
                many_selectors=[
                    "img#image3[alt*='DOWNLOAD LINK']",
                    "img[src*='target']",
                    "a[rel='nofollow'] img#image3",
                    "xpath=//img[@id='image3' and contains(@alt,'DOWNLOAD')]",
                ],
            )
            await wait_for_load(page)
            results.append(StepResult("Step 7", "OK"))

            # Step 8: final Get Link with pre/post waits per spec
            state.current_step = "step 8: pre-wait load + 5s"
            await wait_for_load(page)
            await asyncio.sleep(5)
            await hard_click(
                page,
                "a.get-link",
                "Get Link final",
                many_selectors=[
                    "a.btn.get-link",
                    "a.btn-success.get-link",
                    "a:has-text('Get Link')",
                    "div:has(a.get-link) a",
                    "a[href*='mosco.co.in'][class*='get-link']",
                    "xpath=//a[contains(@class,'get-link') or normalize-space(text())='Get Link']",
                ],
            )
            await asyncio.sleep(1)
            results.append(StepResult("Step 8", "OK", "Waited 5s before click; 1s after click"))

            # Step 9: close browser after 1s post-click wait
            state.current_step = "closing"
            return results
        finally:
            with contextlib.suppress(Exception):
                if context is not None:
                    await context.close()
            with contextlib.suppress(Exception):
                if browser is not None:
                    await browser.close()
            elapsed = time.time() - start_time
            results.append(StepResult("Done", "OK", f"Elapsed: {elapsed:.1f}s"))


async def main():
    file_path = r"urls.txt"
    try:
        urls_list = [ln.strip() for ln in Path(file_path).read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    except Exception as e:
        console.print(f"[bold red]Failed to read URL file:[/bold red] {e}")
        sys.exit(1)
    if not urls_list:
        console.print("[bold red]No URLs found in the file[/bold red]")
        sys.exit(1)

    # Ask for number of instances (1-50)
    try:
        user_in = console.input("[bold cyan]How many instances to run (1-50)? [/bold cyan]")
        n = int(user_in.strip())
        if n < 1 or n > 50:
            raise ValueError
        global INSTANCES
        INSTANCES = n
    except Exception:
        INSTANCES = 1

    states: list[InstanceState] = [InstanceState(id=i+1) for i in range(INSTANCES)]
    total_done = 0

    def pick_url() -> str:
        return random.choice(urls_list)

    def make_dashboard() -> Table:
        table = Table(title="Automation Dashboard", header_style="bold magenta")
        table.add_column("Inst", style="cyan", no_wrap=True)
        table.add_column("Status", style="white")
        table.add_column("Current Step", style="yellow")
        table.add_column("Runs", style="white")
        table.add_column("OK", style="green")
        table.add_column("Fail", style="red")
        table.add_column("Last URL", style="bright_blue")
        table.add_column("Last Detail", style="white")
        for s in states:
            table.add_row(
                str(s.id), s.status, s.current_step, str(s.runs), str(s.successes), str(s.failures), s.last_url[:40] + ("…" if len(s.last_url) > 40 else ""), s.last_detail
            )
        # Totals row
        table.add_row(
            "Totals",
            "",
            "",
            str(sum(s.runs for s in states)),
            str(sum(s.successes for s in states)),
            str(sum(s.failures for s in states)),
            "Ongoing: " + str(sum(1 for s in states if s.status == 'running')),
            "",
        )
        return table

    async def runner_task(state: InstanceState):
        nonlocal total_done
        while True:
            state.status = "running"
            state.current_step = "pick url"
            url = pick_url()
            state.last_url = url
            user_data_dir = Path('.')  # unused (no persistence)
            state.started_at = time.time()
            try:
                results = await run_once(url, state.id, user_data_dir, state)
                state.runs += 1
                state.successes += 1
                total_done += 1
                state.last_detail = results[-1].detail if results else ""
                state.status = "idle"
                state.current_step = "sleep 1s"
                await asyncio.sleep(1)
            except Exception as e:
                state.runs += 1
                state.failures += 1
                state.status = "error/restarting"
                state.last_detail = str(e)
                # No persistence; nothing to clean
                await asyncio.sleep(0.5)

    # Start runners
    runners = [asyncio.create_task(runner_task(s)) for s in states]

    # Live dashboard loop only (no extra prints)
    from rich.live import Live
    try:
        with Live(make_dashboard(), console=console, refresh_per_second=4) as live:
            while True:
                live.update(make_dashboard(), refresh=True)
                await asyncio.sleep(0.25)
    except (KeyboardInterrupt, SystemExit):
        # Graceful shutdown: cancel runners
        for t in runners:
            t.cancel()
    finally:
        with contextlib.suppress(Exception):
            await asyncio.gather(*runners, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Graceful shutdown without stack traces
        pass
