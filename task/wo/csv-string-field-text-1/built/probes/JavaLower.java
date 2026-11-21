import java.nio.file.*;
import java.util.*;

public class JavaLower {
    static String unesc(String s) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length() && s.charAt(i + 1) == 'u' && s.charAt(i + 2) == '{') {
                int end = s.indexOf('}', i);
                b.appendCodePoint(Integer.parseInt(s.substring(i + 3, end), 16));
                i = end;
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }
    static String esc(String s) {
        StringBuilder b = new StringBuilder();
        s.codePoints().forEach(cp -> {
            if (cp >= 0x20 && cp < 0x7f && cp != '\\' && cp != '"') b.appendCodePoint(cp);
            else b.append(String.format("\\u{%x}", cp));
        });
        return b.toString();
    }
    public static void main(String[] a) throws Exception {
        System.err.println("locale " + Locale.getDefault() + " java " + System.getProperty("java.version"));
        for (String line : Files.readAllLines(Paths.get(a[0]))) {
            if (line.isEmpty()) continue;
            String s = unesc(line);
            System.out.println(esc(s) + "\t" + esc(s.toLowerCase()));
        }
    }
}
