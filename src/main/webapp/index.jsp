<%@ page language="java" contentType="text/html; charset=UTF-8"
         pageEncoding="UTF-8"%>
<html>
<script type="text/javascript" src="${ctx}"></script>
<script type="text/javascript" src="/resource/sc/report.js"></script>
<body>
<a href="/seckill/seckill/list"><h2>Hello 秒杀!</h2></a>
</body>
<script type="text/javascript">
    var text = "${user.userName }";
    waterMarker.set(text);
</script>
</html>

