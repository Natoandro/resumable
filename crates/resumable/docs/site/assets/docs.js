(() => {
	const pages = [
		{ group: 'Using the library', href: 'index.html', label: 'Overview' },
		{ group: 'Using the library', href: 'using-the-core.html', label: 'Operations and steps' },
		{
			group: 'Using the library',
			href: 'retries-and-effects.html',
			label: 'Retries and external effects'
		},
		{ group: 'Implementation', href: 'execution-model.html', label: 'Execution state model' },
		{ group: 'Implementation', href: 'store-contract.html', label: 'Store and ownership' },
		{ group: 'Implementation', href: 'records-and-values.html', label: 'Records and values' }
	];
	const path = location.pathname.split('/').pop() || 'index.html';
	const content = document.querySelector('#doc-content article');
	if (!content) return;

	const header = document.createElement('header');
	header.className = 'site-header';
	header.innerHTML = `
		<a class="brand" href="index.html" aria-label="resumable documentation home">
			<span class="brand-mark" aria-hidden="true">rs</span>
			<span class="brand-name">resumable</span>
			<span class="brand-context">Rust library guide</span>
		</a>
		<span class="header-spacer"></span>
		<span class="version-tag">v0.1</span>
		<button class="nav-toggle" type="button" aria-expanded="false" aria-controls="doc-navigation">Contents</button>
	`;
	document.body.prepend(header);

	const skip = document.createElement('a');
	skip.className = 'skip-link';
	skip.href = '#doc-content';
	skip.textContent = 'Skip to documentation';
	document.body.prepend(skip);

	const layout = document.querySelector('.docs-layout');
	const nav = document.createElement('nav');
	nav.className = 'doc-navigation';
	nav.id = 'doc-navigation';
	nav.setAttribute('aria-label', 'Documentation');
	for (const group of [...new Set(pages.map((page) => page.group))]) {
		const section = document.createElement('section');
		section.className = 'nav-group';
		const heading = document.createElement('h2');
		heading.className = 'nav-heading';
		heading.textContent = group;
		const list = document.createElement('ul');
		list.className = 'nav-links';
		for (const page of pages.filter((item) => item.group === group)) {
			const item = document.createElement('li');
			const link = document.createElement('a');
			link.href = page.href;
			link.textContent = page.label;
			if (page.href === path) link.setAttribute('aria-current', 'page');
			item.append(link);
			list.append(item);
		}
		section.append(heading, list);
		nav.append(section);
	}
	layout.prepend(nav);

	const toggle = header.querySelector('.nav-toggle');
	toggle.addEventListener('click', () => {
		const open = toggle.getAttribute('aria-expanded') !== 'true';
		toggle.setAttribute('aria-expanded', String(open));
		nav.dataset.open = String(open);
	});
	nav.addEventListener('click', (event) => {
		if (event.target.closest('a')) {
			nav.dataset.open = 'false';
			toggle.setAttribute('aria-expanded', 'false');
		}
	});
	nav.addEventListener('keydown', (event) => {
		if (event.key === 'Escape') {
			nav.dataset.open = 'false';
			toggle.setAttribute('aria-expanded', 'false');
			toggle.focus();
		}
	});

	const figures = [...content.querySelectorAll('.figure')];
	const updateFigureScrolling = () => {
		const narrow = window.matchMedia('(max-width: 760px)').matches;
		figures.forEach((figure) => {
			if (narrow) {
				figure.tabIndex = 0;
				if (!figure.querySelector('.figure-hint')) {
					const hint = document.createElement('span');
					hint.className = 'figure-hint';
					hint.textContent =
						' On narrow screens, focus this diagram and scroll horizontally to inspect it.';
					figure.querySelector('figcaption')?.append(hint);
				}
			} else {
				figure.removeAttribute('tabindex');
			}
		});
	};
	updateFigureScrolling();
	window.addEventListener('resize', updateFigureScrolling);

	const toc = document.createElement('aside');
	toc.className = 'page-toc';
	toc.setAttribute('aria-label', 'On this page');
	const tocHeading = document.createElement('h2');
	tocHeading.className = 'toc-heading';
	tocHeading.textContent = 'On this page';
	const tocList = document.createElement('ul');
	tocList.className = 'toc-links';
	content.querySelectorAll('h2, h3').forEach((heading, index) => {
		if (!heading.id) heading.id = `section-${index + 1}`;
		const item = document.createElement('li');
		const link = document.createElement('a');
		link.href = `#${heading.id}`;
		link.textContent = heading.textContent;
		if (heading.tagName === 'H3') link.classList.add('toc-sub');
		item.append(link);
		tocList.append(item);
	});
	toc.append(tocHeading, tocList);
	layout.append(toc);

	content.querySelectorAll('pre').forEach((pre) => {
		const button = document.createElement('button');
		button.type = 'button';
		button.className = 'copy-button';
		button.textContent = 'Copy code';
		button.setAttribute('aria-label', 'Copy code example');
		button.addEventListener('click', async () => {
			try {
				await navigator.clipboard.writeText(pre.querySelector('code')?.innerText ?? pre.innerText);
				button.textContent = 'Copied';
			} catch {
				button.textContent = 'Select and copy';
			}
			window.setTimeout(() => {
				button.textContent = 'Copy code';
			}, 1800);
		});
		pre.append(button);
	});
})();
