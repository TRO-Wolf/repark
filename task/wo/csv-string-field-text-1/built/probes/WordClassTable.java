import java.util.ArrayList;
import java.util.List;

public class WordClassTable {
    static boolean isCased(int ch) {
        int type = Character.getType(ch);
        if (type == Character.LOWERCASE_LETTER || type == Character.UPPERCASE_LETTER
                || type == Character.TITLECASE_LETTER) {
            return true;
        }
        return (ch >= 0x02B0 && ch <= 0x02B8) || (ch >= 0x02C0 && ch <= 0x02C1)
            || (ch >= 0x02E0 && ch <= 0x02E4) || ch == 0x0345 || ch == 0x037A
            || (ch >= 0x1D2C && ch <= 0x1D61) || (ch >= 0x2160 && ch <= 0x217F)
            || (ch >= 0x24B6 && ch <= 0x24E9);
    }

    static boolean cjk(int c) {
        return c == 0x3005 || (c >= 0x4e00 && c <= 0x9fa5) || (c >= 0xf900 && c <= 0xfa2d)
            || (c >= 0x30a1 && c <= 0x30fa) || c == 0x30fd || c == 0x30fe
            || (c >= 0x3041 && c <= 0x3094) || c == 0x309d || c == 0x309e
            || (c >= 0x3099 && c <= 0x309c) || c == 0x30fb || c == 0x30fc;
    }

    static int wordClass(int c) {
        int t = Character.getType(c);
        boolean cased = isCased(c);
        if (c == 0x00ad || c == 0x2027) {
            return 9;
        }
        if (t == Character.FORMAT) {
            return 7;
        }
        if (t == Character.NON_SPACING_MARK || t == Character.ENCLOSING_MARK) {
            return cased ? 4 : 3;
        }
        boolean letterBase = t == Character.UPPERCASE_LETTER || t == Character.LOWERCASE_LETTER
            || t == Character.TITLECASE_LETTER || t == Character.MODIFIER_LETTER
            || t == Character.OTHER_LETTER || t == Character.COMBINING_SPACING_MARK;
        if (letterBase && !cjk(c)) {
            return cased ? 2 : 1;
        }
        if (t == Character.DECIMAL_DIGIT_NUMBER || t == Character.LETTER_NUMBER
                || t == Character.OTHER_NUMBER) {
            return cased ? 6 : 5;
        }
        if (c == 0x0964 || c == 0x0965) {
            return 8;
        }
        if (c == '.') {
            return 11;
        }
        if (c == '"' || c == '\'') {
            return 10;
        }
        if (c == ',' || c == 0x066b) {
            return 12;
        }
        if (t == Character.DASH_PUNCTUATION || t == Character.CONNECTOR_PUNCTUATION) {
            return 9;
        }
        if (c == '%' || c == '&' || c == 0x00a2 || c == 0x066a || c == 0x2030 || c == 0x2031) {
            return 14;
        }
        if (t == Character.CURRENCY_SYMBOL || c == '#') {
            return 13;
        }
        return 0;
    }

    public static void main(String[] args) {
        List<String> runs = new ArrayList<>();
        int previous = -1;
        for (int c = 0; c <= 0x10FFFF; c++) {
            int k = wordClass(c);
            if (k != previous) {
                runs.add(String.format("0x%x", (c << 4) | k));
                previous = k;
            }
        }
        System.out.println("#[allow(clippy::unreadable_literal)]");
        System.out.println("const WORD_CLASS_RUNS: &[u32] = &[" + String.join(", ", runs) + "];");
        System.err.println("runs " + runs.size());
    }
}
