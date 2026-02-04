/**
 * ORION Dashboard - Custom JavaScript
 * Handles interactions, animations, and HTMX events
 */

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    initMobileMenu();
    initTooltips();
    initModals();
    initHTMXEvents();
});

/**
 * Mobile Menu Toggle
 */
function initMobileMenu() {
    const sidebar = document.querySelector('.sidebar');
    const menuBtn = document.getElementById('mobile-menu-btn');

    if (menuBtn && sidebar) {
        menuBtn.addEventListener('click', () => {
            sidebar.classList.toggle('open');
        });

        // Close on click outside
        document.addEventListener('click', (e) => {
            if (!sidebar.contains(e.target) && !menuBtn.contains(e.target)) {
                sidebar.classList.remove('open');
            }
        });
    }
}

/**
 * Tooltip Initialization
 */
function initTooltips() {
    // Tooltips are handled via CSS [data-tooltip] attribute
    // This function is a placeholder for future JS-based tooltips if needed
}

/**
 * Modal Handling
 */
function initModals() {
    // Open modal
    document.querySelectorAll('[data-modal-open]').forEach(btn => {
        btn.addEventListener('click', () => {
            const modalId = btn.getAttribute('data-modal-open');
            const modal = document.getElementById(modalId);
            if (modal) {
                modal.classList.add('active');
                document.body.style.overflow = 'hidden';
            }
        });
    });

    // Close modal
    document.querySelectorAll('.modal-close, [data-modal-close]').forEach(btn => {
        btn.addEventListener('click', () => {
            const modal = btn.closest('.modal-backdrop');
            if (modal) {
                modal.classList.remove('active');
                document.body.style.overflow = '';
            }
        });
    });

    // Close on backdrop click
    document.querySelectorAll('.modal-backdrop').forEach(backdrop => {
        backdrop.addEventListener('click', (e) => {
            if (e.target === backdrop) {
                backdrop.classList.remove('active');
                document.body.style.overflow = '';
            }
        });
    });

    // Close on Escape key
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal-backdrop.active').forEach(modal => {
                modal.classList.remove('active');
                document.body.style.overflow = '';
            });
        }
    });
}

/**
 * HTMX Event Handlers
 */
function initHTMXEvents() {
    // Before request - show loading state
    document.body.addEventListener('htmx:beforeRequest', (e) => {
        const target = e.detail.elt;
        if (target.classList.contains('btn')) {
            target.classList.add('loading');
            target.disabled = true;
        }
    });

    // After request - hide loading state
    document.body.addEventListener('htmx:afterRequest', (e) => {
        const target = e.detail.elt;
        if (target.classList.contains('btn')) {
            target.classList.remove('loading');
            target.disabled = false;
        }
    });

    // Handle redirect headers from server
    document.body.addEventListener('htmx:beforeSwap', (e) => {
        const xhr = e.detail.xhr;
        const redirect = xhr.getResponseHeader('HX-Redirect');
        if (redirect) {
            window.location.href = redirect;
            e.detail.shouldSwap = false;
        }
    });

    // Handle errors
    document.body.addEventListener('htmx:responseError', (e) => {
        showToast('An error occurred. Please try again.', 'error');
    });

    // Close modals after successful watchlist add from modal search
    document.body.addEventListener('htmx:afterSwap', (e) => {
        // Check if this was a watchlist toggle from the add-stock-modal
        if (e.detail.target.id === 'modal-search-results' ||
            e.detail.target.closest('#modal-search-results')) {
            // Find the modal and close it
            const modal = document.getElementById('add-stock-modal');
            if (modal) {
                modal.classList.remove('active');
                document.body.style.overflow = '';
            }
            // Refresh the stocks table
            const stocksTable = document.getElementById('stocks-table');
            if (stocksTable) {
                htmx.ajax('GET', '/stocks', {target: '#stocks-table', source: stocksTable});
            }
        }
    });
}

/**
 * Toast Notifications
 */
function showToast(message, type = 'info', duration = 5000) {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <div class="flex items-start gap-3">
            <div class="flex-1">${message}</div>
            <button class="toast-close" onclick="this.parentElement.parentElement.remove()">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="18" y1="6" x2="6" y2="18"/>
                    <line x1="6" y1="6" x2="18" y2="18"/>
                </svg>
            </button>
        </div>
    `;

    container.appendChild(toast);

    // Auto remove after duration
    setTimeout(() => {
        toast.style.animation = 'toast-out 0.3s ease forwards';
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

// Expose showToast globally
window.showToast = showToast;

/**
 * Close add stock modal helper
 */
function closeAddStockModal() {
    const modal = document.getElementById('add-stock-modal');
    if (modal) {
        modal.classList.remove('active');
        document.body.style.overflow = '';
    }
}

window.closeAddStockModal = closeAddStockModal;

/**
 * Number Formatting
 */
function formatNumber(num, options = {}) {
    const {
        style = 'decimal',
        currency = 'USD',
        compact = false,
        decimals = 2
    } = options;

    const formatter = new Intl.NumberFormat('en-US', {
        style: style,
        currency: currency,
        notation: compact ? 'compact' : 'standard',
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });

    return formatter.format(num);
}

window.formatNumber = formatNumber;

/**
 * Format currency values
 */
function formatCurrency(value, compact = true) {
    return formatNumber(value, {
        style: 'currency',
        currency: 'USD',
        compact: compact
    });
}

window.formatCurrency = formatCurrency;

/**
 * Format percentage values
 */
function formatPercent(value, decimals = 2) {
    const sign = value >= 0 ? '+' : '';
    return `${sign}${value.toFixed(decimals)}%`;
}

window.formatPercent = formatPercent;

/**
 * Animate number counting
 */
function animateValue(element, start, end, duration = 1000) {
    const range = end - start;
    const startTime = performance.now();

    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);

        // Easing function (ease-out)
        const easeOut = 1 - Math.pow(1 - progress, 3);
        const current = start + (range * easeOut);

        element.textContent = formatNumber(current);

        if (progress < 1) {
            requestAnimationFrame(update);
        }
    }

    requestAnimationFrame(update);
}

window.animateValue = animateValue;
