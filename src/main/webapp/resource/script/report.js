//var fForm = $("#editFlow"), wIndex;
//function fillForm(data) {
//    fForm.find("input").each(function () {
//        $(this).val(data[$(this).attr("name")]);
//    });
//}
//
//function clearForm() {
//    fForm.find("input").each(function () {
//        //初始化为0
//        $(this).val(0);
//        if ($(this).attr("name") && $(this).attr("name") == "remark") {
//            $(this).val("");
//        }
//    });
//    $(".form-group").each(function () {
//        $(this).removeClass("has-error").removeClass("has-success");
//    });
//    $(".help-block").each(function () {
//        $(this).remove();
//    })
//    return;
//}

queryTypeChange();
$("#queryType").on('change', function () {
    queryTypeChange();
});

function queryTypeChange() {
    var queryType = $("#queryType").val();
    debugger;
    switch (queryType) {
        case "detailData" :
            $("[module='detail']").show();
            $("[module='day']").hide();
            $("[module='week']").hide();
            $("[module='month']").hide();
            break;
        case "dayData" :
            $("[module='detail']").hide();
            $("[module='day']").show();
            $("[module='week']").hide();
            $("[module='month']").hide();
            break;
        case "weekMonthData" :
            $("[module='detail']").hide();
            $("[module='day']").hide();
            $("[module='week']").show();
            $("[module='month']").show();
            break;
    }
}