document.addEventListener('DOMContentLoaded', function() {
    const scrollText = document.getElementById('scroll-text');
    const bottomScrollText = document.getElementById('bottom-scroll-text');

    // Fetch and update the top banner text from data.json
    function fetchTopBannerData() {
        fetch('/static/data/data.json')
        .then(response => response.json())
        .then(data => updateTopBannerText(data))
        .catch(error => console.error('Error fetching top banner data:', error));
    }

    function updateTopBannerText(data) {
        let content = '';
        data.forEach(item => {
            const change = item.new_uv - item.initial_uv;
            const percentage = ((change / item.initial_uv) * 100).toFixed(2);
            const isPositive = change >= 0;
            const colorClass = isPositive ? 'neon-green' : 'neon-red';
            const symbol = isPositive ? '↑' : '↓';
            const randomValue = (15 + Math.random() * 39).toFixed(3);

            content += `<span class="${colorClass}" style="margin-right: 50px;">${item.product_name} ${symbol}${percentage}% ${randomValue}</span>`;
        });
        scrollText.innerHTML = content;
    }

    // Fetch and update the bottom banner text from bottom-banner-data.json
    function fetchBottomBannerData() {
        fetch('/static/data/bottom-banner-data.json')
        .then(response => response.json())
        .then(data => updateBottomBannerText(data))
        .catch(error => console.error('Error fetching bottom banner data:', error));
    }

    function updateBottomBannerText(data) {
        let content = '';
        if (data.text) {
            content = `<span style="color: white;">${data.text}</span>`;
        }
        bottomScrollText.innerHTML = content;
    }

    fetchTopBannerData();
    fetchBottomBannerData();
    setInterval(fetchTopBannerData, 30000); // Fetch new top banner data every 30 seconds
    setInterval(fetchBottomBannerData, 30000); // Fetch new bottom banner data every 30 seconds
});

