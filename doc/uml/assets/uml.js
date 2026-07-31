// Page chrome only: theme override + nav highlighting. Diagrams are static SVG.
(function () {
  var KEY = 'abasift-uml-theme';
  var ORDER = ['auto', 'light', 'dark'];
  var btn = document.getElementById('theme');

  function apply(mode) {
    if (mode === 'auto') document.documentElement.removeAttribute('data-theme');
    else document.documentElement.setAttribute('data-theme', mode);
    if (btn) btn.textContent = 'theme: ' + mode;
  }

  var saved = null;
  try { saved = localStorage.getItem(KEY); } catch (e) { /* file:// with no storage */ }
  apply(ORDER.indexOf(saved) >= 0 ? saved : 'auto');

  if (btn) {
    btn.addEventListener('click', function () {
      var cur = document.documentElement.getAttribute('data-theme') || 'auto';
      var next = ORDER[(ORDER.indexOf(cur) + 1) % ORDER.length];
      apply(next);
      try { localStorage.setItem(KEY, next); } catch (e) { /* ignore */ }
    });
  }

  var links = {};
  Array.prototype.forEach.call(document.querySelectorAll('.nav a'), function (a) {
    links[a.getAttribute('href').slice(1)] = a;
  });
  var sections = document.querySelectorAll('h2[id]');
  if (!sections.length || !window.IntersectionObserver) return;

  var visible = {};
  var io = new IntersectionObserver(function (entries) {
    entries.forEach(function (e) { visible[e.target.id] = e.isIntersecting; });
    var current = null;
    Array.prototype.forEach.call(sections, function (s) {
      if (visible[s.id] && !current) current = s.id;
    });
    Object.keys(links).forEach(function (id) {
      links[id].classList.toggle('active', id === current);
    });
  }, { rootMargin: '0px 0px -70% 0px' });
  Array.prototype.forEach.call(sections, function (s) { io.observe(s); });
})();
