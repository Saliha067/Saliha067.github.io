# My Blog - Quick Start Guide

## 📁 Project Structure

```
my-blog/
├── config.yml              # Main configuration - CUSTOMIZE THIS
├── content/
│   ├── about.md           # About page - CUSTOMIZE THIS
│   ├── search.md          # Search page (keep as-is)
│   └── posts/             # Your blog posts go here
│       └── 18-tracking... # Example post (you can delete after learning)
├── static/
│   ├── images/            # Your blog images
│   ├── fonts/             # Custom fonts
│   ├── robot.txt          # SEO robots file
│   └── site.webmanifest   # PWA manifest
├── layouts/               # Custom HTML templates
├── themes/PaperMod/       # Hugo theme (git submodule)
└── .github/workflows/     # GitHub Actions for deployment
```

## 🚀 Local Development

### Start the blog locally:
```bash
cd "/Users/mythily/Documents/Saliha/Blog i need/my-blog"
hugo server -D
```

Then open: **http://localhost:1313**

Press `Ctrl+C` to stop the server.

## ✏️ Customization Checklist

### 1. Update `config.yml` (look for `# ===== CUSTOMIZE:` comments):
- [ ] Change `baseURL` to your GitHub Pages URL
- [ ] Update `title` with your name
- [ ] Replace `author` with your name
- [ ] Update `description` and `keywords`
- [ ] Update social media links (GitHub, LinkedIn, etc.)
- [ ] Change `editPost.URL` to your repository
- [ ] Update `homeInfoParams` with your bio

### 2. Update `content/about.md`:
- [ ] Write your personal bio
- [ ] Add your contact information
- [ ] Share your background and interests

### 3. Add your first blog post:
```bash
hugo new posts/my-first-post.md
```

Then edit `content/posts/my-first-post.md` with your content.

### 4. Replace favicons (optional):
Generate new favicons at https://favicon.io and replace files in `static/`:
- favicon.ico
- favicon-16x16.png
- favicon-32x32.png
- apple-touch-icon.png
- android-chrome-512x512.png

## 📝 Creating New Posts

### Method 1: Use Hugo command
```bash
hugo new posts/my-new-post.md
```

### Method 2: Create file manually
Create `content/posts/my-post.md`:

```markdown
---
title: "My Post Title"
date: 2025-12-24T10:00:00+00:00
draft: false
ShowToc: true
TocOpen: false
summary: "Brief description of your post"
tags: ["tag1", "tag2"]
categories: ["category"]
---

Your content here...
```

## 🖼️ Adding Images

1. Create a folder in `static/images/` for your post:
   ```bash
   mkdir -p static/images/my-post-name/
   ```

2. Add your images there

3. Reference in your markdown:
   ```markdown
   ![Image description](/images/my-post-name/image.png)
   ```

## 🌐 Deployment to GitHub Pages

### One-time setup:
1. Create a new GitHub repository
2. Enable GitHub Pages in repository settings:
   - Go to Settings → Pages
   - Source: GitHub Actions

3. Update `config.yml` with your repository URL
4. Update `.github/workflows/hugo.yml` if needed

### Deploy:
```bash
git add .
git commit -m "Initial blog setup"
git remote add origin https://github.com/yourusername/your-repo.git
git push -u origin main
```

GitHub Actions will automatically build and deploy your site!

## 🎨 Theme Customization

The blog uses the [PaperMod](https://github.com/adityatelange/hugo-PaperMod) theme.

- Theme files: `themes/PaperMod/`
- Custom overrides: `layouts/`
- Custom CSS/JS: `layouts/partials/extend_head.html`

## 🛠️ Useful Commands

```bash
# Start development server
hugo server -D

# Build the site (output to public/)
hugo

# Build for production with minification
hugo --minify

# Create new post
hugo new posts/post-name.md

# Check Hugo version
hugo version
```

## 📚 Learning Resources

- [Hugo Documentation](https://gohugo.io/documentation/)
- [PaperMod Theme Docs](https://github.com/adityatelange/hugo-PaperMod/wiki)
- [Markdown Guide](https://www.markdownguide.org/)

## ⚡ Tips

1. **Test locally first**: Always run `hugo server -D` to preview changes
2. **Use drafts**: Set `draft: true` in frontmatter while writing
3. **Organize images**: Create separate folders per post in `static/images/`
4. **Git commit often**: Save your progress regularly
5. **Check the example post**: Study `18-tracking-antarctic-giants...` for reference

## 🐛 Troubleshooting

### Site doesn't load?
- Check if Hugo server is running
- Verify `themes/PaperMod/` has files (not empty)
- Check config.yml for syntax errors

### Images not showing?
- Verify image path starts with `/images/`
- Check file exists in `static/images/`
- Image paths are case-sensitive

### Theme not working?
```bash
cd themes/PaperMod
git pull origin master
```

---

**Your blog is ready! Start by customizing config.yml and content/about.md, then create your first post!** 🎉
