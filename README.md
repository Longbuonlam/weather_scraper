# weather_scraper

1. Kiểm tra xem đã cài python chưa : python3 --version
2. Mở visual studio code, mở terminal và gõ 2 câu lệnh : pip3 install openmeteo-requests && pip3 install requests-cache retry-requests numpy pandas
3. Chạy chương trình để lấy data.
4. Bên thứ 3 có giới hạn minute request nên nhớ để ý ở phần terminal log đến lấy data của tỉnh nào thì bị lỗi, nếu : Fetching weather data for Vĩnh Long... Traceback (most recent call last):... -> comment các tỉnh ở phía trên Vĩnh Long, tránh lấy lại data đã có.
5. Nếu lỗi là daily request, chuyển sang mạng khác và tiếp tục.