package org.seckill;

public class EnumConstants {

    public static final String TAT =  "aaccout";
    public static final String TBT =  "baccout";

    public enum Route{
        A(TAT),
        B(TBT),
        ;
        private String route;

        public void setRoute(String route) {
            this.route = route;
        }

        Route(String tat) {
            setRoute(tat);
        }
    }
}
